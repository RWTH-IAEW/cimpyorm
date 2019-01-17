FROM python

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /app

WORKDIR app

CMD ["pytest", "--cov", "--runslow"]