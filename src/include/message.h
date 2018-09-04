#ifndef MESSAGE_H__
#define MESSAGE_H__

// mesage_status defines the status of the previous request.
// FEEL FREE TO ADD YOUR OWN OR REMOVE ANY THAT ARE UNUSED IN YOUR PROJECT
typedef enum message_status {
    OK_DONE,
    OK_WAIT_FOR_RESPONSE,
    UNKNOWN_COMMAND,
    QUERY_UNSUPPORTED,
    OBJECT_ALREADY_EXISTS,
    OBJECT_NOT_FOUND,
    INCORRECT_FORMAT, 
    EXECUTION_ERROR,
    INCORRECT_FILE_FORMAT,
    FILE_NOT_FOUND,
    INDEX_ALREADY_EXISTS
} message_status;

// message is a single packet of information sent between client/server.
// message_status: defines the status of the message.
// length: defines the length of the string message to be sent.
// payload: defines the payload of the message.
typedef struct message {
    message_status status;
    int length;
    char* payload;
} message;

#endif
