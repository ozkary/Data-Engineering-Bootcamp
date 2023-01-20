Install Terraform in GitHub Codespace

VERSION=1.3.7
cd ~
wget "https://releases.hashicorp.com/terraform/"$VERSION"/terraform_"$VERSION"_linux_amd64.zip"
unzip "terraform_"$VERSION"_linux_amd64.zip"
sudo install terraform /usr/local/bin/

Execution
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Open the terraform folder in your project

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
# Delete infra after your work, to avoid costs on any running services
terraform destroy