pragma solidity >=0.5.12<0.6.0;

import "./Channel.sol";

contract ChannelManager {
  uint constant INVALID_UINT = uint(-1);

  event ChannelCreated(uint channelId, address publisher);
  event PublisherJoined(uint channelId, address publisher);
  event MessagePublished(uint channelId, address publisher, uint messageId);
  event ConsumerJoined(
    uint channelId,
    bytes32 consumerGroupId,
    address subscriber
  );
  event MessagePopped(
    uint channelId,
    bytes32 consumerGroupId,
    address subscriber,
    uint messageId
  );
  event MessageSkipped(
    uint channelId,
    bytes32 consumerGroupId,
    address subscriber,
    uint messageId
  );
  event MessageRead(
    uint channelId,
    bytes32 consumerGroupId,
    address subscriber,
    uint messageId
  );

  address public admin;
  address[] public channels;

  modifier onlyValidChannelId(uint _channelId) {
    require(channels.length > _channelId, "Channel does not exist.");

    _;
  }

  constructor() public {
    admin = msg.sender;
  }

  function createChannel(
    bytes memory _xpub,
    uint _index,
    string memory _channelMetadata
  ) public returns (bool) {
    /* Disable unused variable warning. Will be used for xpub check in the future. */
    _index;

    channels.push(address(new Channel(msg.sender, _xpub, _channelMetadata)));

    emit ChannelCreated(channels.length - 1, msg.sender);

    return true;
  }

  function join(uint _channelId, uint _index) public returns (bool) {
    Channel _channel = getChannel(_channelId);

    bytes memory _xpub = _channel.xpub();
    /* Disable unused variables warning. Will be used for xpub check in the future. */
    _xpub;
    _index;

    _channel.join(msg.sender);

    emit PublisherJoined(_channelId, msg.sender);

    return true;
  }

  function publish(uint _channelId, bytes memory _message)
    public
    returns (bool)
  {
    emit MessagePublished(
      _channelId,
      msg.sender,
      getChannel(_channelId).publish(_message, msg.sender)
    );

    return true;
  }

  function subscribe(
    uint _channelId,
    bytes memory _xpub,
    uint _index,
    uint _messageTimeout,
    uint _numOfRetry
  ) public returns (bool) {
    /* Disable unused variable warning. Will be used for xpub check in the future. */
    _index;

    Channel _channel = getChannel(_channelId);
    _channel.subscribe(_xpub, msg.sender, _messageTimeout, _numOfRetry);

    emit ConsumerJoined(
      _channelId,
      _channel.getConsumerGroupId(msg.sender),
      msg.sender
    );

    return true;
  }

  function getNext(uint _channelId, uint _skipDepth) public returns (uint) {
    Channel _channel = getChannel(_channelId);
    (uint _skippedMessageId, uint _messageId) = _channel.getNext(
      msg.sender,
      _skipDepth
    );

    if (_skippedMessageId != INVALID_UINT) {
      emit MessageSkipped(
        _channelId,
        _channel.getConsumerGroupId(msg.sender),
        msg.sender,
        _skippedMessageId
      );
    }

    emit MessagePopped(
      _channelId,
      _channel.getConsumerGroupId(msg.sender),
      msg.sender,
      _messageId
    );

    return _messageId;
  }

  function skip(uint _channelId, uint _messageId) public returns (bool) {
    Channel _channel = getChannel(_channelId);
    _channel.skip(msg.sender, _messageId);

    emit MessageSkipped(
      _channelId,
      _channel.getConsumerGroupId(msg.sender),
      msg.sender,
      _messageId
    );

    return true;
  }

  function confirm(uint _channelId, uint _messageId) public returns (bool) {
    Channel _channel = getChannel(_channelId);
    _channel.confirm(msg.sender, _messageId);

    emit MessageRead(
      _channelId,
      _channel.getConsumerGroupId(msg.sender),
      msg.sender,
      _messageId
    );

    return true;
  }

  function isPublisher(uint _channelId, address _publisher)
    public
    view
    returns (bool)
  {
    return getChannel(_channelId).isPublisher(_publisher);
  }

  function isSubscriber(uint _channelId, address _subscriber)
    public
    view
    returns (bool)
  {
    return getChannel(_channelId).isSubscriber(_subscriber);
  }

  function getMessage(uint _channelId, uint _messageId)
    public
    view
    returns (bytes memory message)
  {
    return getChannel(_channelId).getMessage(_messageId);
  }

  function getChannelMetadata(uint _channelId)
    public
    view
    onlyValidChannelId(_channelId)
    returns (string memory channelMetadata)
  {
    Channel _channel = Channel(getChannel(_channelId));

    return _channel.channelMetadata();
  }

  function getChannel(uint _channelId)
    internal
    view
    onlyValidChannelId(_channelId)
    returns (Channel)
  {
    return Channel(channels[_channelId]);
  }
}
