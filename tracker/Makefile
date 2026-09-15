CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -pthread -g
COMMON   := ../common
SRCS     := main.cpp tracker_state.cpp session.cpp sync.cpp \
            $(COMMON)/net.cpp $(COMMON)/buffer.cpp $(COMMON)/sha1.cpp
OBJS     := $(SRCS:.cpp=.o)
TARGET   := tracker

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
