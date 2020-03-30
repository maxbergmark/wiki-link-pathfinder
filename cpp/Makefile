CC = g++
SRC = $(wildcard src/*.cpp)
OBJ = $(SRC:.cpp=.o)
BIN = ./bin
TARGET = wiki_search.out

LDFLAGS = -lm -fopenmp -D_GLIBCXX_DEBUG
CFLAGS = -I./include -c -fPIC -g -O3 -Wall --std=c++11 -fopenmp -D_GLIBCXX_DEBUG

all: dir $(BIN)/$(TARGET)

dir: ${BIN}

${BIN}:
	mkdir -p $(BIN)

%.o: %.cpp
	$(CC) $(CFLAGS) -c -o $@ $<

$(BIN)/$(TARGET): $(OBJ)
	$(CC) $(LDFLAGS) -o $@ $^

.PHONY: clean
clean:
	rm -f $(OBJ) $(BIN)/$(TARGET)
