FROM python:3.8

# create destination directory
RUN mkdir -p /home/work
WORKDIR /home/work

# copy files
COPY ../streamlit/requirements.txt ./

# Installation
RUN pip3 install -r requirements.txt

# Run command
CMD ["streamlit","run", "streamlit_app.py"]