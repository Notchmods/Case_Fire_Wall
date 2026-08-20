# Obsidio starter: Node. Pinned base image for reproducible builds.
FROM node:20.18-slim
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY server.js ./
EXPOSE 8080
CMD ["node", "server.js"]
