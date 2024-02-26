FROM python:3-alpine AS build
COPY ./ /app
RUN cd /app && pip install build && python -m build -w
RUN apk add --no-cache lzo-dev snappy-dev gcc g++
RUN cd /app/dist && pip wheel --cache-dir ../cache -r ../requirements-ext.txt

FROM python:3-alpine
ENV PYTHONDONTWRITEBYTECODE=1
COPY --from=build /app/dist/*.whl /dist/
RUN apk add --no-cache lzo snappy
RUN --mount=type=cache,target=/root/.cache pip install --no-compile /dist/*.whl
ENTRYPOINT ["log2s3"]
