'use strict';
class FileNotFound extends Error {
    constructor(message){
        super(message);
    }
}

module.exports = FileNotFound;
