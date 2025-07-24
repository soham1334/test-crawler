# Use Node.js as the base image
FROM node:20-alpine

# Install pnpm, rsync, and openssl
RUN corepack enable && corepack prepare pnpm@latest --activate && \
    apk add --no-cache git rsync openssl

# Set up pnpm global bin directory and shell
ENV PNPM_HOME=/root/.local/share/pnpm
ENV PATH=$PNPM_HOME:$PATH
ENV SHELL=/bin/sh
ENV ENV=/root/.ashrc

# Set working directory
WORKDIR /app

# Copy package files
COPY package.json pnpm-lock.yaml ./
COPY .godspeed .

# Install global packages
RUN npm install -g pnpm
RUN pnpm install -g @godspeedsystems/godspeed
RUN pnpm install -g run-script-os

# Install project dependencies
RUN pnpm install

# Copy source code
COPY . .

# Build the application
# RUN godspeed prisma prepare     uncomment if you need to prepare Prisma
RUN godspeed build

# Expose the port the app runs on
EXPOSE 3000
# Start the application
CMD ["godspeed", "serve"]
