
Install Google CLI in Codespace

cd ~
wget "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-414.0.0-linux-x86_64.tar.gz"
# gzip -d "google-cloud-cli-414.0.0-linux-x86_64.tar.gz"
tar -xvf "google-cloud-cli-414.0.0-linux-x86_64.tar.gz"
./google-cloud-sdk/install.sh
# Download your google key file and save in the ~/ root folder
export GOOGLE_APPLICATION_CREDENTIALS="~/file-name.json"

# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Open the browser to get the token and authorize the CLI
