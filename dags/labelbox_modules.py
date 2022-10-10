import requests
import re
import dateutil.parser
import uuid
import mysql.connector
from airflow.models import Variable

# Retrieve Labelbox API Key from Airflow's Variables
LABELBOX_API_KEY = Variable.get("LABELBOX_API_KEY")
HEADERS = {'Authorization': LABELBOX_API_KEY}


# Description: Returns the label information for the first frame of the video clip associated with the uri passed in
def get_label_data(uri):

    # Uri: a url pointing to labeled data information, extracted from the `Labeled Data` field, 
    # received after running process_labels_from_url

    try:
        response = requests.get(uri, headers = HEADERS)
        data = response.text
        label = data.partition('\n')[0] # Grab the first frame of the video only
        return label
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("Oops! Something weird happened...",err)

# Description: Looks at a .txt containing labelbox export information and returns a value from a line given a key (or null if not found)
def get_value_from_key(line, key):

    # Line: line of text file to read
    # Key: key in JSON to search for

    received_answers = []

    # Parse clip labels
    for obj in line:
        # If obj is list of labels, parse individual labels
        if obj == 'Label':
            for label_obj in line[obj]:
                if 'answer' in label_obj:
                    if label_obj['value'] == key:
                        return label_obj['answer']['value'].replace(',', '_')
                if 'answers' in label_obj:
                    for answer in label_obj['answers']:
                        if label_obj['value'] == key:
                            received_answers.append(answer['value'])

        if obj == key:
            return line[key]
    
    if len(received_answers) > 0:
        return "|".join(received_answers)
    else:
        return None

# Description: Connects to DB using mysql.connector library and returns a connection
def rds_database_connect(is_internal_endpoint):
    try:
        if is_internal_endpoint:
            conn = mysql.connector.connect(
                host=Variable.get("DATABASE_ENDPOINT"),
                user=Variable.get("RDS_USER"),
                passwd=Variable.get("RDS_PASSWORD"),
                port=Variable.get("DATABASE_PORT"),
                database=Variable.get("DATABASE_NAME"),
            )
        return conn
    except Exception as e:
        print("Database connection failed due to {}".format(e))


# CHANGING THIS FOR NOW
# ASSUMING THE NEW TABLE IS CALLED CLIPS COPY

# Description: For a given line in the .txt, parse labels and update DB if entry has not been made and entry is not a part of the Sandbox project
def update_db_entry(connection, line):

    # Connection: connection to MySQL server
    # Line: contents of line in .txt

    # Parse values for s3_path and vid_id
    s3_path = get_value_from_key(line, "Labeled Data")
    vid_id_found = re.search("VID(.+?).mp4", s3_path)
    exam_id_found = re.search("https://deepbreathe.s3.ca-central-1.amazonaws.com/(.+?)_", s3_path)
    patient_id_found = re.search("_(.+?)_", s3_path)
    vid_id = None if not vid_id_found else vid_id_found.group(1)
    exam_id = None if not exam_id_found else exam_id_found.group(1)
    patient_id = None if not patient_id_found else patient_id_found.group(1)

    # Schema for clips. Key represents DB column name
    data_clip = {
        "id": "",  # Generate UUID once label determined to be unique
        "exam_id": exam_id,
        "labels_created_at": dateutil.parser.parse(
            get_value_from_key(line, "Created At")
        ),
        "labels_updated_at": dateutil.parser.parse(
            get_value_from_key(line, "Updated At")
        ),
        "view": get_value_from_key(line, "view"),
        "a_or_b_lines": get_value_from_key(line, "a_vs._b_lines"),
        "frame_homogeneity": get_value_from_key(line, "frame_homogeneity"),
        "pleural_effusion": get_value_from_key(line, "pleural_effusion"),
        "consolidation": get_value_from_key(line, "consolidation:"),
        "pleural_line_findings": get_value_from_key(line, "pleural_line_findings"),
        "air_bronchograms": get_value_from_key(line, "air_bronchograms"),
        "curtain_sign": 1
        if get_value_from_key(line, "curtain_sign") == "present"
        else 0,
        "ascites": 1 if get_value_from_key(line, "ascites") == "present" else 0,
        "quality": get_value_from_key(line, "quality"),
        "do_not_use": 1
        if get_value_from_key(line, "do_not_use") == "do_not_use"
        else 0,
        "patient_id": patient_id,
        "vid_id": vid_id,
        "s3_path": s3_path,
        "mirror_image": 1
        if get_value_from_key(line, "mirror_image:") == "present"
        else 0,
        "labelbox_project_number": get_value_from_key(line, "Project Name"),
    }

    # Create cursor via MySQL connection
    cursor = connection.cursor(buffered=True)

    # Grab all vid IDs from DB
    query = (
        """SELECT * FROM clips WHERE vid_id = %s AND exam_id = %s AND patient_id = %s"""
    )
    cursor.execute(
        query,
        (
            data_clip["vid_id"],
            data_clip["exam_id"],
            data_clip["patient_id"],
        ),
    )

    # Update data_clip with appropriate id from database, or a new one
    result = cursor.fetchone()
    if result is not None:
        data_clip["id"] = result[0]
    else:
        data_clip["id"] = str(uuid.uuid4())

    # Query to see if any rows exist with the same vid ID
    project_name = get_value_from_key(line, "Project Name")

    # Upsert data into table
    if cursor.rowcount > 0:
        print(
            "Clip with vid ID {} already in table. Updating entry. (Project: {})".format(
                data_clip["vid_id"], project_name
            )
        )
    else:
        print(
            "Adding clip with vid ID {} to table (Project: {}).".format(
                data_clip["vid_id"], project_name
            )
        )
        print(data_clip)

    # CHANGING THIS FOR NOW
    # ASSUMING THE NEW TABLE IS CALLED CLIPS COPY
    add_clip = """
                INSERT INTO clips_jui
                (id, exam_id, labels_created_at, labels_updated_at, view, a_or_b_lines, frame_homogeneity,
                pleural_effusion, consolidation, pleural_line_findings, air_bronchograms, curtain_sign,
                ascites, quality, do_not_use, patient_id, vid_id, s3_path, mirror_image, labelbox_project_number)
                VALUES (%(id)s, %(exam_id)s, %(labels_created_at)s, %(labels_updated_at)s, %(view)s, %(a_or_b_lines)s,
                %(frame_homogeneity)s, %(pleural_effusion)s, %(consolidation)s, %(pleural_line_findings)s, %(air_bronchograms)s, %(curtain_sign)s,
                %(ascites)s, %(quality)s, %(do_not_use)s, %(patient_id)s, %(vid_id)s, %(s3_path)s, %(mirror_image)s,
                %(labelbox_project_number)s)
                AS new

                ON DUPLICATE KEY UPDATE
                labels_created_at = new.labels_created_at, labels_updated_at = new.labels_updated_at,
                view = new.view, a_or_b_lines = new.a_or_b_lines, frame_homogeneity = new.frame_homogeneity,
                pleural_effusion = new.pleural_effusion, consolidation = new.consolidation,
                pleural_line_findings = new.pleural_line_findings, air_bronchograms = new.air_bronchograms,
                curtain_sign = new.curtain_sign, ascites = new.ascites, quality = new.quality, do_not_use = new.do_not_use,
                mirror_image = new.mirror_image, labelbox_project_number = new.labelbox_project_number
                """

    cursor.execute(add_clip, data_clip)
    connection.commit()

    cursor.close()
