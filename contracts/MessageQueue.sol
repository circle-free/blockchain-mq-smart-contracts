pragma solidity >=0.5.12<0.6.0;

import "./LibLinkedList.sol";

library MessageQueue {
  uint constant INVALID_UINT = uint(-1);

  using LibLinkedList for LibLinkedList.linkedList;

  struct consumerGroup {
    uint offset;
    LibLinkedList.linkedList readDateSortedList;
    LibLinkedList.linkedList skippedDateSortedList;
    uint messageTimeout;
    uint numOfRetry;
    mapping(address => LibLinkedList.linkedList) messagesRead;
    mapping(uint => address) messagesReadBy;
    mapping(uint => uint) numMessagesReadBy;
    mapping(address => bool) haveMessageRead;
    mapping(address => uint) messageRead;
    bool created;
  }

  struct message {
    bytes contents;
    uint lastUpdatedAt;
  }

  struct messageQueue {
    mapping(uint => message) messages;
    uint numOfMessages;
  }

  function create() internal pure returns (messageQueue memory) {
    return messageQueue({numOfMessages: 0});
  }

  function enqueue(messageQueue storage _self, bytes memory _message)
    internal
    returns (uint)
  {
    require(_message.length > 0, "Message cannot be empty.");

    _self.messages[_self.numOfMessages] = message({
      contents: _message,
      lastUpdatedAt: now
    });

    return _self.numOfMessages++;
  }

  function getNext(
    messageQueue storage _self,
    consumerGroup storage _consumerGroup,
    address _subscriber,
    uint _skipDepth
  ) internal returns (uint) {
    if (_consumerGroup.haveMessageRead[_subscriber]) {
      uint _readMessageId = _consumerGroup.messageRead[_subscriber];

      _consumerGroup.readDateSortedList.remove(_readMessageId);
      _consumerGroup.readDateSortedList.add(_readMessageId);
      _self.messages[_readMessageId].lastUpdatedAt = now;
      _consumerGroup.numMessagesReadBy[_readMessageId]++;

      return _readMessageId;
    }

    if (!_consumerGroup.skippedDateSortedList.isEmpty()) {
      uint _oldestSkippedMessageId = _consumerGroup.skippedDateSortedList.head;
      message memory _oldestSkippedMessage = _self.messages[_oldestSkippedMessageId];

      if (now - _oldestSkippedMessage.lastUpdatedAt >= _skipDepth) {
        _consumerGroup.skippedDateSortedList.remove(_oldestSkippedMessageId);
        _consumerGroup.readDateSortedList.add(_oldestSkippedMessageId);
        _self.messages[_oldestSkippedMessageId].lastUpdatedAt = now;

        _consumerGroup.messagesReadBy[_oldestSkippedMessageId] = _subscriber;
        _consumerGroup.haveMessageRead[_subscriber] = true;
        _consumerGroup.messageRead[_subscriber] = _oldestSkippedMessageId;
        _consumerGroup.numMessagesReadBy[_oldestSkippedMessageId] = 1;

        return _oldestSkippedMessageId;
      }
    }

    if (!_consumerGroup.readDateSortedList.isEmpty()) {
      uint _oldestReadMessageId = _consumerGroup.readDateSortedList.head;
      message memory _oldestReadMessage = _self.messages[_oldestReadMessageId];

      if (now - _oldestReadMessage.lastUpdatedAt >= _consumerGroup.messageTimeout) {
        _consumerGroup.readDateSortedList.remove(_oldestReadMessageId);
        _consumerGroup.readDateSortedList.add(_oldestReadMessageId);
        _self.messages[_oldestReadMessageId].lastUpdatedAt = now;

        address _originalReader = _consumerGroup.messagesReadBy[_oldestReadMessageId];
        _consumerGroup.haveMessageRead[_originalReader] = false;
        _consumerGroup.messagesReadBy[_oldestReadMessageId] = _subscriber;
        _consumerGroup.messageRead[_subscriber] = _oldestReadMessageId;
        _consumerGroup.haveMessageRead[_subscriber] = true;
        _consumerGroup.numMessagesReadBy[_oldestReadMessageId]++;

        return _oldestReadMessageId;
      }
    }

    require(
      _consumerGroup.offset != _self.numOfMessages,
      "All messages are read for the moment."
    );

    uint _nextMessageId = _consumerGroup.offset++;
    _consumerGroup.readDateSortedList.add(_nextMessageId);
    _consumerGroup.messageRead[_subscriber] = _nextMessageId;
    _consumerGroup.haveMessageRead[_subscriber] = true;
    _consumerGroup.messagesReadBy[_nextMessageId] = _subscriber;
    _consumerGroup.numMessagesReadBy[_nextMessageId]++;
    _self.messages[_nextMessageId].lastUpdatedAt = now;

    return _nextMessageId;
  }

  function skip(
    messageQueue storage _self,
    consumerGroup storage _consumerGroup,
    address _subscriber,
    uint _messageId
  ) internal {
    require(_self.numOfMessages > _messageId, "Invalid message ID.");
    require(
      _consumerGroup.messagesReadBy[_messageId] == _subscriber,
      "Subscriber not allowed to skip this message."
    );

    _consumerGroup.readDateSortedList.remove(_messageId);
    _consumerGroup.haveMessageRead[_subscriber] = false;
    _consumerGroup.messagesReadBy[_messageId] = address(0);
    _consumerGroup.skippedDateSortedList.add(_messageId);
    _self.messages[_messageId].lastUpdatedAt = now;
  }

  function confirm(
    messageQueue storage _self,
    consumerGroup storage _consumerGroup,
    address _subscriber,
    uint _messageId
  ) internal {
    require(_self.numOfMessages > _messageId, "Invalid message ID.");
    require(
      _consumerGroup.messagesReadBy[_messageId] == _subscriber,
      "Subscriber not allowed to confirm this message."
    );

    _consumerGroup.readDateSortedList.remove(_messageId);
    _consumerGroup.haveMessageRead[_subscriber] = false;
  }

  function getMessage(messageQueue storage _self, uint _messageId)
    internal
    view
    returns (bytes memory)
  {
    require(_self.numOfMessages > _messageId, "Invalid message ID.");

    return _self.messages[_messageId].contents;
  }
}
