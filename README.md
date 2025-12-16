# Msgbuzz

Generic message bus abstraction. Supported implementation: RabbitMQ, Supabase Queue.

# Usage

See folder: `examples`

# Scaling Consumers

Increase the number of consumers of each subscription to improve performance.

You could do this by:
1. Running the consumer file (e.g.: `receive.py` in the examples folder) multiple times.
2. Use Supervisor and set numprocs to a number larger than one.
