# Makefile for installation and management of technologies

# ---------------------
# Docker
# ---------------------
help-docker:
	@echo "Options for Docker:"
	@echo "  docker-install: Installs Docker and Docker Compose on Ubuntu"

docker-install:
	# Update packages
	sudo apt-get update
	# Install Docker
	sudo apt-get install docker.io -y
	# Start and enable Docker
	sudo systemctl start docker
	sudo systemctl enable docker
	# Install Docker Compose
	sudo apt-get install docker-compose -y
	sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
	sudo chmod +x /usr/local/bin/docker-compose
	# Install pip3
	sudo apt install python3-pip

# ---------------------
# Postgres
# ---------------------

help-postgres:
	@echo "Options for Postgres:"
	@echo "  postgres-build: Builds the Postgres image with PostGIS"
	@echo "  postgres-up: Starts the Postgres service"
	@echo "  postgres-down: Stops the Postgres service"
	@echo "  postgres-populate: Populates the Postgres database with events data"

postgres-build:
	docker build -t postgres-postgis postgres/postgis

postgres-up:
	docker-compose -f postgres/docker-compose.postgres.yaml up -d

postgres-down:
	docker-compose -f postgres/docker-compose.postgres.yaml down

postgres-populate:
	python3 -m events --db 'postgres'


# ---------------------
# Kafka
# ---------------------

help-kafka:
	@echo "Options for Kafka:"
	@echo "  kafka-up: Starts the Kafka service (use IP=<ip_address> to define the IP)"
	@echo "  kafka-down: Stops the Kafka service"

kafka-up:
	@if [ -z "$(IP)" ]; then \
		echo "Please, define the IP variable. Example: make kafka-up IP=127.0.0.1"; \
		exit 1; \
	fi
	echo "Usando o endereço IP: $(IP)"
	IP=$(IP) docker-compose --verbose -f kafka/docker-compose.kafka.yaml up -d
kafka-down:
	docker-compose -f kafka/docker-compose.kafka.yaml down

# ---------------------
# Jupyter Notebook
# ---------------------

help-jupyter:
	@echo "Options for Jupyter Notebook:"
	@echo "  jupyter-up: Starts the Jupyter Notebook service (use IP=<ip_address> to define the IP)"
	@echo "  jupyter-down: Stops the Jupyter Notebook service"

jupyter-up:
	@if [ -z "$(IP)" ]; then \
		echo "Please, define the IP variable. Example: make jupyter-up IP=127.0.0.1"; \
		exit 1; \
	fi
	echo "Usando o endereço IP: $(IP)"
	IP=$(IP) docker-compose --verbose -f jupyter/docker-compose.jupyter.yaml up -d

jupyter-down:
	docker-compose -f jupyter/docker-compose.jupyter.yaml down












