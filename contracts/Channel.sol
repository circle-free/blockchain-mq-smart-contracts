pragma solidity >=0.5.12<0.6.0;

import "./LibLinkedList.sol";
import "./MessageQueue.sol";

contract Channel {
  uint constant INVALID_UINT = uint(-1);

  using MessageQueue for MessageQueue.messageQueue;

  address public channelManager;
  mapping(address => bool) public publishers;
  bytes public xpub;
  string public channelMetadata;

  mapping(address => bytes32) private hashedXpubs;
  mapping(bytes32 => MessageQueue.consumerGroup) private consumerGroups;
  MessageQueue.messageQueue private messageQueue;

  modifier onlyPublisher(address _publisher) {
    require(
      publishers[_publisher],
      "Only publishers are allowed to make this transaction."
    );

    _;
  }

  modifier onlyChannelManager() {
    require(
      channelManager == msg.sender,
      "Only channel manager is allowed to access channel."
    );

    _;
  }

  modifier onlySubsriber(address _subscriber) {
    require(
      isSubscriber(_subscriber),
      "Only subscriber is allowed to make this transaction."
    );

    _;
  }

  constructor(address _publisher, bytes memory _xpub, string memory _channelMetadata)
    public
  {
    publishers[_publisher] = true;
    xpub = _xpub;
    channelManager = msg.sender;
    channelMetadata = _channelMetadata;
    messageQueue = MessageQueue.create();
  }

  function join(address _publisher) public onlyChannelManager returns (bool) {
    publishers[_publisher] = true;

    return true;
  }

  function publish(bytes memory _message, address _publisher)
    public
    onlyChannelManager
    onlyPublisher(_publisher)
    returns (uint)
  {
    uint _messageId = messageQueue.enqueue(_message);

    return _messageId;
  }

  function subscribe(
    bytes memory _xpub,
    address _subscriber,
    uint _messageTimeout,
    uint _numOfRetry
  ) public onlyChannelManager returns (bool) {
    bytes32 _hashedXpub = keccak256(_xpub);
    hashedXpubs[_subscriber] = _hashedXpub;
    MessageQueue.consumerGroup storage _consumerGroup = consumerGroups[_hashedXpub];

    require(_messageTimeout > 0, "Invalid message timeout.");

    _consumerGroup.messageTimeout = _messageTimeout;
    _consumerGroup.numOfRetry = _numOfRetry;

    if (!_consumerGroup.messagesRead[_subscriber].created) {
      _consumerGroup.messagesRead[_subscriber] = LibLinkedList.create();
    }

    if (!_consumerGroup.created) {
      _consumerGroup.readDateSortedList = LibLinkedList.create();
      _consumerGroup.skippedDateSortedList = LibLinkedList.create();
      _consumerGroup.created = true;
    }

    return true;
  }

  function getNext(address _subscriber, uint _skipDepth)
    public
    onlyChannelManager
    onlySubsriber(_subscriber)
    returns (uint, uint)
  {
    MessageQueue.consumerGroup storage _consumerGroup = consumerGroups[hashedXpubs[_subscriber]];
    require(_consumerGroup.created, "Consumer group not created.");

    uint _messageId = messageQueue.getNext(
      _consumerGroup,
      _subscriber,
      _skipDepth
    );

    if (_consumerGroup.numMessagesReadBy[_messageId] > _consumerGroup.numOfRetry) {
      messageQueue.skip(_consumerGroup, _subscriber, _messageId);

      return (_messageId, messageQueue.getNext(
        _consumerGroup,
        _subscriber,
        _skipDepth
      ));
    }

    return (INVALID_UINT, _messageId);
  }

  function skip(address _subscriber, uint _messageId)
    public
    onlyChannelManager
    onlySubsriber(_subscriber)
    returns (bool)
  {
    MessageQueue.consumerGroup storage _consumerGroup = consumerGroups[hashedXpubs[_subscriber]];
    require(_consumerGroup.created, "Consumer group not created.");

    messageQueue.skip(_consumerGroup, _subscriber, _messageId);

    return true;
  }

  function confirm(address _subscriber, uint _messageId)
    public
    onlyChannelManager
    onlySubsriber(_subscriber)
    returns (bool)
  {
    MessageQueue.consumerGroup storage _consumerGroup = consumerGroups[hashedXpubs[_subscriber]];
    require(_consumerGroup.created, "Consumer group not created.");

    messageQueue.confirm(_consumerGroup, _subscriber, _messageId);

    return true;
  }

  function isPublisher(address _publisher)
    public
    view
    onlyChannelManager
    returns (bool)
  {
    return publishers[_publisher];
  }

  function isSubscriber(address _subscriber)
    public
    view
    onlyChannelManager
    returns (bool)
  {
    return hashedXpubs[_subscriber] != bytes32(0);
  }

  function getMessage(uint _messageId)
    public
    view
    onlyChannelManager
    returns (bytes memory)
  {
    return messageQueue.getMessage(_messageId);
  }

  function getConsumerGroupId(address _subscriber)
    public
    view
    onlyChannelManager
    returns (bytes32)
  {
    return hashedXpubs[_subscriber];
  }
}
