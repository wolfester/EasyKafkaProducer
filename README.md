# EasyKafkaProducer

##Synopsis
The EasyKafkaProducer is a lightweight, easy to use Kafka producer.  I intended to call this a "simple" producer, but the definition of simple given by Kafka implies that management of Kafka metadata would be done by the calling application.  Instead, I made management of the Kafka metadata part of the library.  

The library fetches metadata as needed to maintain connection strings and topic statuses.  The library also produces in a round-robin fashion to partitions of a topic.  While some may wish to indicate which partition to produce to, I feel like that is out of the scope of this library since that would require metadata being accessible to the calling application.

##Compilation
Compiles as "ekp". 

##Usage
For usage examples, please see the pathwatcher.go and filefollower.go files here: https://github.com/wolfester/LogRocket 

The LogRocket was developed in conjunction with the EasyKafkaProducer and is the best example I have of its use at this time.

## License

MIT License:

Copyright (c) 2016, Charles Wolfe and contributors. All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.