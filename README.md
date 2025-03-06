# R2R Backend Service

#### Prerequisites
Before you begin, ensure you have met the following requirements:
```
Python 3.11+
pip
```
#### Installation
To install the required dependencies, follow these steps:

#### Linux and macOS:
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```
#### Windows:
```
python -m venv env
.\env\Scripts\activate
pip install -r requirements.txt
```

#### Environment Variables (.env file)
```
APP_NAME="R2R"
OPENAI_API_KEY=""
BAM_API_KEY=""
BAM_API_ENDPOINT=""
WATSONX_API_KEY=""
WATSONX_API_ENDPOINT=""
WATSONX_PROJECT_ID=""
```
#### Running the Application
To run the application, use the following command:
```
uvicorn app.main:app --reload --reload-exclude 'bin/*'
```

#### Test API
```
GET: http://127.0.0.1:8000/api/v1/transformation/home

Response
{
	"status": true,
	"message": "Welcome to R2R - Summarization"
}
```