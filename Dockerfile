FROM python:3.9-slim-buster AS builder

RUN apt-get update && apt-get install -y curl && apt-get clean
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /ophelian

COPY pyproject.toml ./

RUN poetry lock

COPY pyproject.toml poetry.lock README.md ./
COPY ophelian ./ophelian

RUN poetry install --no-root --no-dev --no-interaction --no-ansi
RUN poetry build

FROM python:3.9-slim-buster

WORKDIR /ophelian

COPY --from=builder /ophelian/dist /ophelian/dist

RUN pip install /ophelian/dist/*.whl

EXPOSE 8000
CMD ["python", "-m", "ophelian"]
