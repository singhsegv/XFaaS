# Steps

1. Build a zip for 1 basic print function
2. wsk create n actions
3. wsk orchestrator v1: start -> parallel[2, 3, 4] -> 5
4. wsk orchestrator v2: start -> parallel[seq[2, 3], seq[4, 5], 6] -> 7
