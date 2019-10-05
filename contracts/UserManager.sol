pragma solidity >=0.5.12<0.6.0;

contract UserManager {
  event UserRegistered(address userAddress, string username);

  mapping(address => string) private usernames;
  mapping(string => address) private userAddresses;

  address private admin;

  modifier onlyAdmin() {
    require(
      admin == msg.sender,
      "Only admin is allowed to make this transaction."
    );

    _;
  }

  modifier onlyNotRegistered(string memory _username) {
    require(isUserRegistered(msg.sender), "User already registered.");
    require(
      userAddresses[_username] == address(0),
      "Username is already taken."
    );

    _;
  }

  modifier onlyValidUsername(string memory _username) {
    uint _usernameLength = bytes(_username).length;

    require(_usernameLength > 0, "Username cannot be empty.");
    require(
      _usernameLength <= 10,
      "Username can only have maximum length of 10."
    );

    _;
  }

  constructor() public {
    admin = msg.sender;
  }

  function register(string memory _username)
    public
    onlyNotRegistered(_username)
    onlyValidUsername(_username)
    returns (bool)
  {
    usernames[msg.sender] = _username;
    userAddresses[_username] = msg.sender;

    emit UserRegistered(msg.sender, _username);

    return true;
  }

  function getUsername() public view returns (string memory) {
    return usernames[msg.sender];
  }

  function getUserAddress(string memory _username)
    public
    view
    returns (address)
  {
    return userAddresses[_username];
  }

  function isUserRegistered(address _userAddress) public view returns (bool) {
    return bytes(usernames[_userAddress]).length > 0;
  }
}
