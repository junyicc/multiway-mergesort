# Multiway mergesort

It is an implementation using the way of multiway mergesort to sort data.

The basic idea is as follows:

1. generate random records.

2. phase1
    - split all records into pieces.
    - sort pieces separately and concurrently.
    - pass records in each piece through channel to phase2 in separated goroutine.
    
3. phase2
    - construct input buffer, and receive record from channels.
    - select the min record, and put it into output buffer.