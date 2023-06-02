#!/usr/bin/env python
import csv
import argparse
import sys
import pymongo
import getpass
import datetime
import os
import subprocess
import math
from datetime import timedelta
import xlsxwriter

# Parse arguments for job
parser = argparse.ArgumentParser()
parser.add_argument("--files", dest="workFiles", nargs="+", help="files to process")
parser.add_argument("-xytech", dest="xytechFile", help="xytech file to process")
parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose mode')
parser.add_argument('--process', help='The video file to process')
parser.add_argument('--output', dest='xls_file', help='Output XLS file')

# Parse the command-line arguments
args = parser.parse_args()

# Check if workfiles and xytechfile are accessible
if (args.workFiles is None) or (args.xytechFile is None) or (args.process is None) or (args.xls_file is None):
    print("Invalid arguments provided.")
    sys.exit(2)

workFiles = args.workFiles
xytechFile = args.xytechFile

# Create MongoClient
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Create Database
database = client["mydatabase"]
works_collection = database["works_collection"]
frames_collection = database["frames_collection"]

# Work order class object
class WorkOrder:
    def __init__(self, machine, user_on_file, file_date):
        self.user_run_script = getpass.getuser()
        self.machine = machine
        self.user_on_file = user_on_file
        self.file_date = file_date
        self.time_stamp = datetime.datetime.now().replace(microsecond=0)

class FramesOrder:
    def __init__(self, user_on_file, file_date, location_frames):
        self.user_on_file = user_on_file
        self.file_date = file_date
        self.location_frames = location_frames

# Open Xytech file
xytech_folders = []
csv_array = []

read_xytech_file = open(xytechFile, "r")
for line in read_xytech_file:
    if "/" in line:
        xytech_folders.append(line)

# Read each line from Baselight file
for file in workFiles:
    frame_locations_array = []
    name_without_extension = os.path.splitext(file)[0]
    read_baselight_file = open(file, "r")

    for line in read_baselight_file:
        parts = line.split("Avatar/", 1)
        if len(parts) > 1:
            sub_folder = parts[1].strip().split()[0]
        new_location = ""

        # Folder replace check
        for xytech_line in xytech_folders:
            if sub_folder in xytech_line:
                new_location = xytech_line.strip()

        first = ""
        pointer = ""
        last = ""
        for numeral in line.split(" "):
            # Skip <err> and <null>
            if not numeral.strip().isnumeric():
                continue
            # Assign first number
            if first == "":
                first = int(numeral)
                pointer = first
                continue
            # Keeping to range if succession
            if int(numeral) == (pointer + 1):
                pointer = int(numeral)
                continue
            else:
                # Range ends or no succession, output
                last = pointer
                if first == last:
                    innerArray = []
                    innerArray.append(new_location)
                    innerArray.append(str(first))
                    frame_locations_array.append(innerArray)
                else:
                    innerArray = []
                    innerArray.append(new_location)
                    innerArray.append('%s-%s' % (first, last))
                    frame_locations_array.append(innerArray)
                first = int(numeral)
                pointer = first
                last = ""

        # Working with last number each line
        last = pointer
        if first != "":
            if first == last:
                innerArray = []
                innerArray.append(new_location)
                innerArray.append(str(first))
                frame_locations_array.append(innerArray)
            else:
                innerArray = []
                innerArray.append(new_location)
                innerArray.append('%s-%s' % (first, last))
                frame_locations_array.append(innerArray)

    file_names = name_without_extension.split("_")

    work_order = WorkOrder(file_names[0], file_names[1], file_names[2])
    works_collection.insert_one(work_order.__dict__)

    frames_order = FramesOrder(file_names[1], file_names[2], frame_locations_array)
    frames_collection.insert_one(frames_order.__dict__)

    csv_array.append([file, ''])
    for locations in frame_locations_array:
        csv_array.append(locations)

def export_csv():
    with open("frames-data.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(['locations', 'frames'])
        writer.writerows(csv_array)

export_csv()

# Function to calculate timecode from frames
def calculate_timecode(frames, frame_rate):
    hours = int(frames / ((frame_rate * 60) * 60))
    minutes = int(frames / (frame_rate * 60)) % 60
    seconds = int((frames % (frame_rate * 60)) / frame_rate)
    frame_remains = frames % (frame_rate * 60) % frame_rate
    return ("%02d:%02d:%02d:%02d" % (hours, minutes, seconds, frame_remains))

# Function to fetch frame rate from file
def fetch_frame_rate(file_path):
    command = ["ffprobe", "-v", "0", "-of", "csv=p=0", "-select_streams", "v:0", "-show_entries", "stream=r_frame_rate", file_path]
    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    frame_rate = int(process.stdout.decode("utf-8").strip().replace("/1", ""))
    return frame_rate

# Function to fetch total frames from file
def fetch_total_frames(file_path):
    command = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-count_packets", "-show_entries", "stream=nb_read_packets", "-of", "csv=p=0", file_path]
    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    total_frames = int(process.stdout.decode("utf-8").strip())
    return total_frames

# Function to extract frame from file
def extract_frame(file_path, time, output_name):
    command = ["ffmpeg", "-i", file_path, "-ss", time, "-vf", "scale=96:74", "-frames:v", "1", "-q:v", "2", output_name]
    subprocess.run(command)

# MongoClient Connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["mydatabase"]
frames_collection = database["frames_collection"]

frame_rate = fetch_frame_rate(args.process)
total_frames = fetch_total_frames(args.process)

frame_documents = frames_collection.find({})
location_ranges = []
for doc in frame_documents:
    for location_frame in doc["location_frames"]:
        location, frame_range = location_frame
        start_frame, end_frame = map(int, frame_range.split("-") if "-" in frame_range else (frame_range, frame_range))
        if start_frame != end_frame and start_frame <= total_frames and end_frame <= total_frames:
            location_ranges.append([location, start_frame, end_frame])

workbook = xlsxwriter.Workbook(args.xls_file)
worksheet = workbook.add_worksheet()
row = 0
col = 0

for location, start_frame, end_frame in location_ranges:
    start_timecode = calculate_timecode(start_frame, frame_rate)
    end_timecode = calculate_timecode(end_frame, frame_rate)
    avg_frame = math.ceil((start_frame + end_frame) / 2)
    avg_time = str(timedelta(seconds=(avg_frame / frame_rate)))

    extract_frame(args.process, avg_time, f"{row}.jpg")

    worksheet.write(row, col, location)
    worksheet.write(row, col + 1, f"{start_frame}-{end_frame}")
    worksheet.write(row, col + 2, f"{start_timecode}-{end_timecode}")
    worksheet.insert_image(row, col + 3, f"{row}.jpg")
    row += 1

workbook.close()
