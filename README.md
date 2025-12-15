# Msgbuzz

Generic message bus abstraction. Supported implementation: RabbitMQ, Supabase Queue.

# Usage

See folder: `examples`

# Scaling Subscriber

Increase the number of consumers of each subscription to improve performance.

You could do this by:
1. Running the subscriber file (e.g.: `receive.py` in the examples folder) multiple times.
2. Use Supervisor and set numprocs to a number larger than one.
