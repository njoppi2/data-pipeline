# Prepare the Docker Compose setup
init:
	mkdir -p ./dags ./logs ./plugins ./config

# Start the Docker Compose setup
up:
	docker compose up

# Stop the Docker Compose setup
down:
	docker compose down

# All starting commands
start: init up

# All stopping commands
stop: down

# Clean up the Docker Compose setup
clean:
	rm -rf ./dags ./logs ./plugins ./config
