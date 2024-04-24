# imports
import paramiko
import datetime
import sqlite3
import logging
import base64
import os
import shutil
import subprocess
import pandas as pd
import datetime
from datetime import datetime , timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import ssl



# Take .wav files from server
class Input:
    def __init__(self, log_file_path='logs/file_convert.log'):
        self.log_file_path = log_file_path
        self.setup_logging()
        self.setup_ssh_client()

    def setup_logging(self):
        if not os.path.exists(self.log_file_path):
            os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
            with open(self.log_file_path, 'w'):
                pass
        logging.basicConfig(filename=self.log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def setup_ssh_client(self):
        self.host = self.decode_variable(os.environ.get('SSH_HOST'))
        self.username = self.decode_variable(os.environ.get('SSH_USERNAME'))
        self.password = self.decode_variable(os.environ.get('SSH_PASSWORD'))
        self.port = int(self.decode_variable(os.environ.get('SSH_PORT', '22')))
        
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            self.ssh_client.connect(hostname=self.host, port=self.port, username=self.username, password=self.password)
            logging.info("SSH connection established.")
        except Exception as e:
            logging.error(f"Error establishing SSH connection: {str(e)}")

    def decode_variable(self, encoded_variable):
        decoded_variable = base64.b64decode(encoded_variable.encode()).decode()
        return decoded_variable

    def download_files(self):
        if not hasattr(self, 'ssh_client') or not hasattr(self, 'host'):
            logging.error("SSH client not initialized.")
            return
        
        try:
            # Open SFTP connection
            ftp = self.ssh_client.open_sftp()
            logging.info("SFTP connection established.")
            
            # Define the remote directory path
            remote_directory = "/home/trellissoft/temp_files/"

            # Get today's date in the format YYMMDD
            today_date_yymmdd = datetime.today().strftime("%y%m%d")

            # Get today's date in the format YYYY-MM-DD
            today_date_yyyymmdd = datetime.today().strftime("%Y-%m-%d")

            # Define default directories
            default_directories = ['input', 'processing', 'completed', 'failed', 'deleted', 'reports', 'logs']
            
            # Create directories if they don't exist
            for directory in default_directories:
                if not os.path.exists(directory):
                    os.makedirs(directory)
                    logging.info(f"Directory '{directory}' created.")
            
            # Check if the same folder exists remotely
            remote_folder_path = os.path.join(remote_directory, today_date_yymmdd)
            try:
                folder_attributes = ftp.stat(remote_folder_path)
                logging.info(f"The folder '{remote_folder_path}' exists remotely.")
                
                # Connect to SQLite database
                conn = sqlite3.connect('conversion.db')
                cursor = conn.cursor()
                
                # Create table if not exists
                cursor.execute('''CREATE TABLE IF NOT EXISTS SourceFile 
                                  (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                  source_file TEXT, 
                                  local_file TEXT, 
                                  file_size INTEGER, 
                                  status TEXT, 
                                  download_datetime TEXT,
                                  updated_datetime TEXT)''')
                
                # Download .wav files if the folder exists
                wav_files = [file for file in ftp.listdir(remote_folder_path) if file.endswith('.wav')]
                if wav_files:
                    for wav_file in wav_files:
                        # Check if the file has been downloaded today
                        cursor.execute('''SELECT * FROM SourceFile WHERE source_file=? AND download_datetime LIKE ?''', (wav_file, f'{today_date_yyyymmdd}%'))
                        existing_entry = cursor.fetchone()
                        
                        # If the file hasn't been downloaded today, download it
                        if not existing_entry:
                            local_download_directory = os.path.join(os.getcwd(), 'input', today_date_yymmdd)
                            if not os.path.exists(local_download_directory):
                                os.makedirs(local_download_directory)
                                logging.info(f"Local directory '{local_download_directory}' created.")
                            
                            remote_file_path = os.path.join(remote_folder_path, wav_file)
                            local_file_path = os.path.join(local_download_directory, wav_file)
                            
                            try:
                                ftp.get(remote_file_path, local_file_path)
                                logging.info(f"Downloaded '{wav_file}' from '{remote_file_path}' to '{local_file_path}'.")
                                    
                                # Insert record into SQLite database
                                file_size = ftp.stat(remote_file_path).st_size
                                download_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                relative_file_path = os.path.relpath(local_file_path, os.getcwd())
                                cursor.execute('''INSERT INTO SourceFile 
                                                  (source_file, local_file, file_size, status, download_datetime, updated_datetime) 
                                                  VALUES (?, ?, ?, ?, ?, ?)''', 
                                                  (wav_file, relative_file_path, file_size, 'pending', download_datetime, download_datetime))
                                conn.commit()
                                logging.info(f"Inserted '{wav_file}' into the database.")
                            except Exception as e:
                                logging.error(f"Error downloading '{wav_file}': {str(e)}")
                        else:
                            logging.info(f"Skipping '{wav_file}' as it has already been downloaded today.")
                else:
                    logging.info(f"No .wav files found in '{remote_folder_path}'.")
            except FileNotFoundError:
                logging.error(f"The folder '{remote_folder_path}' does not exist remotely.")
                
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            
        finally:
            # Close connections
            if 'ftp' in locals():
                ftp.close()
            if 'conn' in locals():
                conn.close()



#convert .wav audio files to .wma and chunks
class AudioProcessor:
    def __init__(self, log_file='logs/file_convert.log', input_folder="input/", processing_folder="processing/"):
        self.log_file = log_file
        self.input_folder = input_folder
        self.processing_folder = processing_folder
        self.setup_logging()

    def setup_logging(self):
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        logging.basicConfig(filename=self.log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def create_chunks(self, input_file, output_folder, chunk_duration=10):
        try:
            # Connect to SQLite database
            conn = sqlite3.connect('conversion.db')
            cursor = conn.cursor()

            # Create table if not exists
            cursor.execute('''CREATE TABLE IF NOT EXISTS ProcessedFile 
                              (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                              local_file TEXT, 
                              source_file TEXT, 
                              status TEXT, 
                              chunk_created_datetime TEXT,
                              updated_datetime TEXT)''')
            conn.commit()

            # Update database with processing status and start datetime
            start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Extract the filename without the path
            file_name = os.path.basename(input_file)
            cursor.execute('''SELECT * FROM SourceFile WHERE source_file=?''', (file_name,))
            row = cursor.fetchone()
            if row:
                # Entry exists, update it
                cursor.execute('''UPDATE SourceFile 
                                  SET status=?, updated_datetime=? 
                                  WHERE source_file=?''', 
                               ('processing', start_datetime, file_name))
            else:
                # Entry doesn't exist, create it
                cursor.execute('''INSERT INTO SourceFile (source_file, status, updated_datetime) 
                                  VALUES (?, ?, ?)''', (file_name, 'processing', start_datetime))
            conn.commit()

            # Create folder for the input file
            today_date = datetime.now().strftime("%y%m%d")
            file_name_no_extension = os.path.splitext(file_name)[0]
            file_folder = os.path.join(output_folder, today_date, file_name_no_extension)
            os.makedirs(file_folder, exist_ok=True)

            # Create original folder and copy the original WAV file
            original_folder = os.path.join(file_folder, "original")
            os.makedirs(original_folder, exist_ok=True)
            original_file_destination = os.path.join(original_folder, file_name)
            shutil.copy(input_file, original_file_destination)

            # Convert folder
            convert_folder = os.path.join(file_folder, "Convert")
            os.makedirs(convert_folder, exist_ok=True)

            # Chunks folder
            chunks_folder = os.path.join(file_folder, "Chunks")
            os.makedirs(chunks_folder, exist_ok=True)

            # Check if WMA file already exists
            output_file_wma = os.path.join(convert_folder, f"{file_name_no_extension}.wma")
            if os.path.exists(output_file_wma):
                logging.info(f"Skipping conversion for '{file_name}', WMA file already exists.")
                # Update status of the source file to 'processed'
                end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute('''UPDATE SourceFile 
                                  SET status=?, updated_datetime=? 
                                  WHERE source_file=?''', 
                               ('Completed', end_datetime, file_name))
                conn.commit()
                return

            # Convert WAV to WMA using FFmpeg
            logging.info(f"Converting '{file_name}' to WMA format.")
            subprocess.run(['ffmpeg', '-y', '-i', input_file, '-acodec', 'wmav2', output_file_wma], check=True)

            # Extract audio duration
            duration = subprocess.check_output(['ffprobe', '-i', output_file_wma, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=%s' % ("p=0")])
            duration = float(duration.strip())

            # Calculate number of chunks
            total_chunks = int(duration / chunk_duration)
            logging.info(f"Creating {total_chunks} chunks for '{file_name}'.")

            # Iterate through chunks
            for i in range(total_chunks):
                # Calculate start and end time for the chunk
                start_time = i * chunk_duration
                end_time = start_time + chunk_duration

                # Create filename for the chunk
                chunk_name = f"{file_name_no_extension}_{start_time}_{end_time}.wma"
                output_file_chunk = os.path.join(chunks_folder, chunk_name)

                # Extract the chunk using FFmpeg
                logging.info(f"Extracting chunk {i + 1}/{total_chunks} for '{file_name}'.")
                subprocess.run(['ffmpeg', '-y', '-i', output_file_wma, '-acodec', 'copy', '-ss', str(start_time), '-to', str(end_time), output_file_chunk], check=True)

                # Update ProcessedFile table
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute('''INSERT INTO ProcessedFile 
                                  (local_file, source_file, status, chunk_created_datetime, updated_datetime) 
                                  VALUES (?, ?, ?, ?, ?)''', 
                               (os.path.basename(output_file_chunk), file_name, 'Processing', now, now))
                conn.commit()

            # Update status of the source file to 'Completed'
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''UPDATE SourceFile 
                              SET status=?, updated_datetime=? 
                              WHERE source_file=?''', 
                           ('Completed', end_datetime, file_name))
            conn.commit()

            # Close the database connection
            conn.close()
        except Exception as e:
            logging.error(f"Error processing file {input_file}: {str(e)}")
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''UPDATE SourceFile 
                              SET status=?, updated_datetime=? 
                              WHERE source_file=?''', 
                           ('Failed', end_datetime, file_name))
            conn.commit()

            try:
                error_file_folder = os.path.join(self.processing_folder, today_date, file_name_no_extension)
                failed_folder = os.path.join(os.getcwd(), "failed")
                if not os.path.exists(failed_folder):
                    os.makedirs(failed_folder)
                if os.path.exists(error_file_folder):
                    shutil.move(error_file_folder, failed_folder)
                    logging.info(f"Error file '{file_name}' moved to 'failed' folder.")
            except Exception as e:
                logging.error(f"Error moving error file to 'Failed' folder: {str(e)}")

    def process_audio_files(self):
        try:
            # Get current date
            today_date = datetime.now().strftime("%y%m%d")

            # Get list of files in today's date folder in the input directory
            input_folder_today = os.path.join(self.input_folder, today_date)
            if not os.path.exists(input_folder_today):
                logging.info(f"No files found for today's date {today_date} in the input folder.")
                return
            
            files = os.listdir(input_folder_today)

            # Iterate through each file
            for file in files:
                # Check if it's a WAV file
                if file.endswith(".wav"):
                    # Create chunks from the audio file
                    self.create_chunks(os.path.join(input_folder_today, file), self.processing_folder)
        except Exception as e:
            logging.error(f"Error processing audio files: {str(e)}")



# move files to completed after execution
class FileHandler:
    def __init__(self, log_file='logs/file_convert.log', input_folder="input/", processing_folder="processing/", completed_folder="completed/"):
        self.log_file = log_file
        self.input_folder = input_folder
        self.processing_folder = processing_folder
        self.completed_folder = completed_folder
        self.setup_logging()

    def setup_logging(self):
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        logging.basicConfig(filename=self.log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def move_files_to_completed(self):
        try:
            # Get current date
            today_date = datetime.now().strftime("%y%m%d")

            # Path to today's processing folder
            processing_folder_today = os.path.join(self.processing_folder, today_date)
            if not os.path.exists(processing_folder_today):
                logging.info(f"No files found for today's date {today_date} in the processing folder.")
                return
            
            # Path to today's completed folder
            completed_folder_today = os.path.join(self.completed_folder, today_date)
            os.makedirs(completed_folder_today, exist_ok=True)

            # Connect to SQLite database
            conn = sqlite3.connect('conversion.db')
            cursor = conn.cursor()

            # Get list of files in today's processing folder
            files = os.listdir(processing_folder_today)

            # Iterate through each file
            for file_name in files:
                # Move file to completed folder
                source_path = os.path.join(processing_folder_today, file_name)
                destination_path = os.path.join(completed_folder_today, file_name)
                logging.info(f"Moving file {file_name} to completed folder...")

                # Check if source file exists
                if os.path.exists(source_path):
                    try:
                        # Move file
                        shutil.move(source_path, destination_path)
                        logging.info(f"File {file_name} moved to completed folder.")

                        # Update SourceFile table
                        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        cursor.execute('''UPDATE SourceFile 
                                          SET status=?, updated_datetime=? 
                                          WHERE source_file=?''', 
                                       ('Done', now, file_name + ".wav"))
                        conn.commit()

                        # Path to chunks folder for the current file
                        file_folder = os.path.join(completed_folder_today, file_name)
                        chunks_folder = os.path.join(file_folder, "Chunks")
                        
                        # Check if chunks folder exists
                        if os.path.exists(chunks_folder):
                            # Get list of files in the chunks folder
                            chunks_files = os.listdir(chunks_folder)
                            
                            # Iterate through each chunk file
                            for chunk_file in chunks_files:
                                try:
                                    # Update status and updated_datetime columns in ProcessedFile table for chunk file
                                    cursor.execute('''UPDATE ProcessedFile 
                                                    SET status=?, updated_datetime=? 
                                                    WHERE local_file=?''', 
                                                    ('Completed', now, chunk_file))
                                    conn.commit()
                                except Exception as e:
                                    logging.error(f"Error updating ProcessedFile table for chunk {chunk_file}: {str(e)}")
                    except Exception as e:
                        logging.error(f"Error moving file {file_name}: {str(e)}")
                else:
                    logging.error(f"Source file {source_path} does not exist.")

            # Close the database connection
            conn.close()
            logging.info("All files processed successfully.")
        except Exception as e:
            logging.error(f"Error moving files to completed folder: {str(e)}")

    def delete_input_folder_contents(self):
        try:
            # Get current date
            today_date = datetime.now().strftime("%y%m%d")

            # Path to today's input folder
            input_folder_today = os.path.join(self.input_folder, today_date)

            # Check if the input folder exists
            if os.path.exists(input_folder_today):
                # Iterate through files and directories in the input folder
                for item in os.listdir(input_folder_today):
                    item_path = os.path.join(input_folder_today, item)
                    if os.path.isfile(item_path):
                        # Delete file
                        os.remove(item_path)
                        logging.info(f"File '{item}' deleted from input folder.")
                    elif os.path.isdir(item_path):
                        # Delete directory
                        shutil.rmtree(item_path)
                        logging.info(f"Directory '{item}' deleted from input folder.")
            else:
                logging.info(f"No files found for today's date {today_date} in the input folder.")
        except Exception as e:
            logging.error(f"Error deleting input folder contents: {str(e)}")


# make report from database
class DataExporter:
    def __init__(self, db_file="conversion.db", reports_folder="reports/"):
        self.db_file = db_file
        self.reports_folder = reports_folder
        self.setup_logging()

    def setup_logging(self):
        os.makedirs(os.path.dirname(self.reports_folder), exist_ok=True)
        logging.basicConfig(filename='logs/file_convert.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def export_source_file_data_to_excel(self):
        try:
            # Get current date
            today_date = datetime.now().strftime("%Y-%m-%d")
            
            # Connect to SQLite database
            conn = sqlite3.connect(self.db_file)

            # Query to select data from SourceFile table for today's date
            query = f"SELECT * FROM SourceFile WHERE substr(updated_datetime, 1, 10) = '{today_date}'"
            
            # Read data into a DataFrame
            df = pd.read_sql_query(query, conn)

            # Close the database connection
            conn.close()

            # Write DataFrame to Excel file
            excel_file_path = os.path.join(self.reports_folder, f"Report_{today_date}.xlsx")
            df.to_excel(excel_file_path, index=False)

            logging.info(f"Data exported to Excel file: {excel_file_path}")
        except Exception as e:
            logging.error(f"Error exporting data to Excel file: {str(e)}")


# send mail
class EmailSender:
    def __init__(self, db_file="conversion.db"):
        self.db_file = db_file
        self.setup_logging()

    def setup_logging(self):
        log_file = 'logs/file_convert.log'
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_file_count(self, table, status_column=None, status=None, date_column=None, date=None):
        try:
            # Connect to SQLite database
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            # Query to get file count based on status and/or date
            if status_column and status and date_column and date:
                query = f"SELECT COUNT(*) FROM {table} WHERE {status_column} = ? AND substr({date_column}, 1, 10) = ?"
                cursor.execute(query, (status, date))
            elif status_column and status:
                query = f"SELECT COUNT(*) FROM {table} WHERE {status_column} = ?"
                cursor.execute(query, (status,))
            elif date_column and date:
                query = f"SELECT COUNT(*) FROM {table} WHERE substr({date_column}, 1, 10) = ?"
                cursor.execute(query, (date,))
            else:
                query = f"SELECT COUNT(*) FROM {table}"
                cursor.execute(query)
                
            count = cursor.fetchone()[0]

            # Close the database connection
            conn.close()

            return count
        except Exception as e:
            logging.error(f"Error getting file count: {str(e)}")
            return None

    def send_email(self, sender_email, receiver_email, cc_emails, password, subject, body, attachment_paths=None):
        try:
            # Setup the MIME
            message = MIMEMultipart()
            message['From'] = sender_email
            message['To'] = receiver_email
            message['Cc'] = ", ".join(cc_emails) if cc_emails else ""
            message['Subject'] = subject

            # Add body to email
            message.attach(MIMEText(body, 'plain'))

            if attachment_paths:
                for attachment_path in attachment_paths:
                    # Open file in binary mode
                    with open(attachment_path, 'rb') as attachment:
                        # Add file as application/octet-stream
                        # Email client can usually download this automatically as attachment
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())

                    # Encode file in ASCII characters to send by email    
                    encoders.encode_base64(part)

                    # Add header as key/value pair to attachment part
                    part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(attachment_path)}')

                    # Add attachment to message
                    message.attach(part)

            # Create secure connection with server and send email
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(sender_email, password)
                server.sendmail(sender_email, [receiver_email] + cc_emails, message.as_string())

            logging.info("Email sent successfully")
        except Exception as e:
            logging.error(f"Error sending email: {str(e)}")

    def send_daily_status_email(self):
        try:
            # Get today's date
            today_date = datetime.now().strftime("%Y-%m-%d")

            # Get total file count
            total_files = self.get_file_count("SourceFile", date_column="updated_datetime", date=today_date)

            # Get counts for processed, failed, and deleted files
            processed_files = self.get_file_count("SourceFile", status_column="status", status="Done", date_column="updated_datetime", date=today_date)
            failed_files = self.get_file_count("SourceFile", status_column="status", status="Failed", date_column="updated_datetime", date=today_date)
            deleted_files = self.get_file_count("SourceFile", status_column="status", status="Deleted", date_column="updated_datetime", date=today_date)

            # Function to decode Base64 encoded variables
            def decode_variable(encoded_variable):
                decoded_variable = base64.b64decode(encoded_variable.encode()).decode()
                return decoded_variable

            # Email content
            sender_email = decode_variable(os.environ.get('EMAIL_SENDER'))
            receiver_email = decode_variable(os.environ.get('EMAIL_RECEIVER'))
            cc_emails = decode_variable(os.environ.get('EMAIL_CC', '')).split(',') # Split multiple emails by comma
            password = decode_variable(os.environ.get('EMAIL_PASSWORD'))
            subject = "Daily Status Report"
            body = f"Date: {today_date}\nTotal files: {total_files}\nProcessed files: {processed_files}\nFailed files: {failed_files}\nDeleted files: {deleted_files}"
            attachment_paths = [
                f"reports/Report_{today_date}.xlsx",  
                "logs/file_convert.log"  
            ]

            # Send email
            self.send_email(sender_email, receiver_email, cc_emails, password, subject, body, attachment_paths)
        except Exception as e:
            logging.error(f"Error sending daily status email: {str(e)}")

# deletion of files after 24 hours
class FolderManager:
    def __init__(self, db_file="conversion.db"):
        self.db_file = db_file
        self.setup_logging()

    def setup_logging(self):
        log_file = 'logs/file_convert.log'
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def move_processed_folders_to_deleted(self):
        try:
            # Constants
            completed_folder = "completed/"
            deleted_folder = "deleted/"

            # Get current date and time
            current_datetime = datetime.now()

            # Calculate the threshold datetime (24 hours ago)
            threshold_datetime = current_datetime - timedelta(days=1)

            # Connect to SQLite database
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            # Get the list of processed folders that are older than 24 hours
            cursor.execute('''SELECT DISTINCT source_file, DATE(chunk_created_datetime) 
                              FROM ProcessedFile 
                              WHERE DATE(chunk_created_datetime) <= ? 
                              AND status = ?''', 
                           (threshold_datetime.date(), 'Completed'))
            folders_to_delete = cursor.fetchall()

            # If no folders need to be deleted, exit the function
            if not folders_to_delete:
                logging.info("No folders to delete. Exiting the function.")
                return

            # Path to deleted folder
            deleted_folder_path = os.path.join(deleted_folder)

            # Create the deleted folder if it doesn't exist
            os.makedirs(deleted_folder_path, exist_ok=True)
            # Move processed folders to deleted folder
            for source_file, download_datetime in folders_to_delete:
                    # Path to the completed folder
                    date_object = datetime.strptime(download_datetime, '%Y-%m-%d')  
                    completed_folder_path = os.path.join(completed_folder, date_object.strftime("%y%m%d"))  
                    # Move the folder to the deleted folder
                    destination_path = os.path.join(deleted_folder_path)
                    shutil.move(completed_folder_path, destination_path)
                    logging.info(f"Moved folder {completed_folder_path} to deleted folder.")

            # If all folders are moved successfully, update the status in the SourceFile table to "deleted"
            cursor.execute('''UPDATE SourceFile 
                              SET status=?, updated_datetime=? 
                              WHERE source_file IN (SELECT DISTINCT source_file FROM ProcessedFile 
                                                    WHERE DATE(chunk_created_datetime) <= ? 
                                                    AND status = ?)''', 
                           ('Deleted', current_datetime.strftime("%Y-%m-%d %H:%M:%S"), threshold_datetime.date(), 'Completed'))
            conn.commit()
            logging.info("Updated status in SourceFile table to 'Deleted'.")

            # Close the database connection
            conn.close()
            logging.info("Processed folders moved to deleted folder successfully.")
        except Exception as e:
            logging.error(f"Error moving processed folders to deleted folder: {str(e)}")



if __name__ == "__main__":
    # Create an instance of FileConverter
    file_input = Input()

    # Create an instance of AudioProcessor
    audio_processor = AudioProcessor()

    # Create an instance of FileHandler
    file_handler = FileHandler()

     # Create an instance of DataExporter
    data_exporter = DataExporter()

    # Create an instance of EmailSender
    email_sender = EmailSender()

    # Create an instance of FolderManager
    folder_manager = FolderManager()


    
    # Call the download_files method to execute the functionality
    file_input.download_files()

    # Call the process_audio_files method to execute the functionality
    audio_processor.process_audio_files()

    # Call the method to move files to completed folder and update database
    file_handler.move_files_to_completed()

    # Call the method to delete everything from the input folder
    file_handler.delete_input_folder_contents()

    # Call the method to export source file data to Excel
    data_exporter.export_source_file_data_to_excel()

    # Call the method to send daily status email
    email_sender.send_daily_status_email()

    # Call the method to move processed folders to the deleted folder
    folder_manager.move_processed_folders_to_deleted()
