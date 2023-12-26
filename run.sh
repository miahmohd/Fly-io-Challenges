#!/bin/bash

set -xe
# cat ./input  | cargo r

cargo b


# Challenge 1
# ./maelstrom/maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10


# Challenge 2
# ./maelstrom/maelstrom test -w unique-ids --bin ./target/debug/uid --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition



# Challenge 3a
# ./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 1 --time-limit 20 --rate 10

# Challenge 3b
# ./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10

# Challenge 3c
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition