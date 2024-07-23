FROM python:3.9-slim-buster AS builder

RUN apt-get update && apt-get install -y curl && apt-get clean
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /ophelian

COPY pyproject.toml poetry.lock ./

RUN pip install --upgrade pip
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction --no-ansi

COPY ophelian ./ophelian

FROM python:3.9-slim-buster

WORKDIR /ophelian

COPY --from=builder /ophelian /ophelian

EXPOSE 8000

CMD ["python", "-m", "ophelian"]
