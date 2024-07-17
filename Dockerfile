FROM python:3.9-slim-buster AS builder

RUN apt-get update && apt-get install -y curl && apt-get clean
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /ophelia

COPY pyproject.toml poetry.lock ./

RUN pip install --upgrade pip
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction --no-ansi

COPY ophelia_spark ./ophelia

FROM python:3.9-slim-buster

WORKDIR /ophelia

COPY --from=builder /ophelia /ophelia

EXPOSE 8000

CMD ["python", "-m", "ophelia"]
