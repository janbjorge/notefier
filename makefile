up:
	docker-compose -f docker-compose.yml up --build -V --timeout 30

down:
	docker-compose down -v
