FROM python:3.11-slim

WORKDIR /app
COPY server.py index.html sw.js icon-192.png icon-512.png /app/
RUN mkdir -p /app/cache

ENV PORT=8888
ENV PLEX_URL=http://127.0.0.1:32400
ENV TAUTULLI_DB=/tautulli/tautulli.db

EXPOSE 8888
CMD ["python3", "-u", "/app/server.py"]
