# Reflection

## Points of Reflection
1. Ordering -- Relaxed versus SeqCst
2. Registry actor pattern
  - Thought about having a state struct in server that server owns. 
  - Ended up going with actor pattern to have better separation of concerns.