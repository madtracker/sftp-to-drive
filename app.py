from flask import Flask, request, jsonify
import paramiko
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
import tempfile

app = Flask(__name__)

@app.route('/sftp-to-drive', methods=['POST'])
def sftp_to_drive():
    # Get payload from Apps Script
    data = request.get_json()
    sftp_host = data.get('sftpHost')
    sftp_port = data.get('sftpPort', 22)
    sftp_user = data.get('sftpUser')
    sftp_password = data.get('sftpPassword')
    remote_dir = data.get('remoteDir')
    file_names = data.get('fileNames', [])
    folder_id = data.get('folderId')

    # Load credentials
    credentials_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not credentials_json:
        return jsonify({"error": "Credentials not found"}), 400
    with open("/tmp/credentials.json", "w") as f:
        f.write(credentials_json)

    creds = Credentials.from_service_account_file("/tmp/credentials.json", scopes=["https://www.googleapis.com/auth/drive"])
    drive_service = build("drive", "v3", credentials=creds)

    # SFTP connection
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(sftp_host, port=sftp_port, username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()

    with tempfile.TemporaryDirectory() as temp_dir:
        for file_name in file_names:
            local_path = os.path.join(temp_dir, file_name)
            sftp.get(os.path.join(remote_dir, file_name), local_path)
            media = MediaFileUpload(local_path, mimetype="text/csv")
            drive_service.files().create(body={"name": file_name, "parents": [folder_id]}, media_body=media, fields="id").execute()
            print(f"Uploaded {file_name}")

    sftp.close()
    ssh.close()
    return jsonify({"message": "Files uploaded successfully"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT"))  # Remove default 5000
    app.run(host="0.0.0.0", port=port)
