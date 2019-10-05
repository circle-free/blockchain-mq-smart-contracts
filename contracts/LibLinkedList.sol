pragma solidity >=0.5.12<0.6.0;

library LibLinkedList {
  uint constant INVALID_UINT = uint(-1);

  struct node {
    uint id;
    uint priv;
    uint next;
    bool exist;
  }

  struct linkedList {
    uint head;
    uint tail;
    mapping(uint => node) nodes;
    uint length;
    bool created;
  }

  function create() internal pure returns (linkedList memory) {
    return linkedList({
      head: INVALID_UINT,
      tail: INVALID_UINT,
      length: 0,
      created: true
    });
  }

  function isEmpty(linkedList storage _self) internal view returns (bool) {
    return _self.length == 0;
  }

  function size(linkedList storage _self) internal view returns (uint) {
    return _self.length;
  }

  function add(linkedList storage _self, uint _id) internal {
    require(!_self.nodes[_id].exist, "ID being added already exists.");

    _self.nodes[_id].id = _id;
    _self.nodes[_id].priv = INVALID_UINT;
    _self.nodes[_id].next = INVALID_UINT;
    _self.nodes[_id].exist = true;

    if (_self.length == 0) {
      /* If list is empty */
      _self.head = _id;
      _self.tail = _id;
    } else if (_self.length == 1) {
      _self.nodes[_self.head].next = _id;
      _self.nodes[_id].priv = _self.head;
      _self.tail = _id;
    } else {
      /* If list is not empty */
      _self.nodes[_self.tail].next = _id;
      _self.nodes[_id].priv = _self.tail;
      _self.tail = _id;
    }

    _self.length++;
  }

  function remove(linkedList storage _self, uint _id) internal {
    require(_self.nodes[_id].exist, "ID being removed does not exist.");

    if (_id == _self.head) {
      /* Is head */
      _self.head = _self.nodes[_self.nodes[_self.head].next].id;
      _self.nodes[_self.head].priv = INVALID_UINT;
    } else if (_id == _self.tail) {
      /* Is tail */
      _self.tail = _self.nodes[_self.nodes[_self.tail].priv].id;
      _self.nodes[_self.tail].next = INVALID_UINT;
    } else {
      node memory _curNode = _self.nodes[_id];
      _self.nodes[_curNode.priv].next = _curNode.next;
      _self.nodes[_curNode.next].priv = _curNode.priv;
    }

    delete _self.nodes[_id];
    _self.length--;
  }
}
