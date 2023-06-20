# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file into the container and install the necessary packages
COPY requirements.txt ./
#RUN pip install scikit-learn
#RUN pip install scikit-surprise==1.1.1
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Flask app files into the container
COPY movies/ ./movies/
# COPY movies/ /app/movies/

# Set the environment variable for Flask
# ENV FLASK_APP=movies/app.py
ENV FLASK_APP=movies/recommend.py


# Expose the Flask port
EXPOSE 5000

# Run the Flask app
CMD ["flask", "run", "--host=0.0.0.0"]