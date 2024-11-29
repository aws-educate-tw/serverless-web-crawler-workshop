# deploy.sh
#!/bin/bash

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Create package directory
rm -rf package/
mkdir package/

# Install dependencies
pip install -r requirements.txt -t package/

# Copy source code
cp -r src/* package/

# Create deployment package
cd package
zip -r ../function.zip .
cd ..

# Cleanup
rm -rf package/

echo "Deployment package created: function.zip"