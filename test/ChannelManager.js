'use strict';

const chai = require('chai');
chai.use(require('chai-as-promised'));
chai.use(require('chai-shallow-deep-equal'));
const { expect } = chai;
const { mapValues } = require('lodash');
const {
  utils: { asciiToHex },
} = require('web3');

const sleep = second => new Promise(resolve => setTimeout(resolve, second * 1000));

const propertiesBigNumberToNumber = obj =>
  mapValues(obj, value => (typeof value === 'object' ? value.toNumber() : value));

const expectOneEventFromCall = async (expectedEvent, expectedArgs, call) => {
  const { logs } = await call;

  expect(logs).to.have.lengthOf(1);
  const [{ event, args }] = logs;
  expect(event).to.equal(expectedEvent);
  expect(propertiesBigNumberToNumber(args)).to.shallowDeepEqual(expectedArgs);
};

contract('ChannelManager', accounts => {
  const [admin, publisherA, publisherB, subscriberA, subscriberB, subscriberC] = accounts;

  const ChannelManager = artifacts.require('ChannelManager');
  const publisherXpub = asciiToHex(
    'xpub79Gmy5EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw'
  );
  const consumerGroupId = '0xa150fea1970761454b63077f703bc489057d20b1653febe4077eb3b1941640ad';
  const publisherAIndex = 10;
  const publisherBIndex = 11;
  const channelMetadata = 'someMetadata';

  const testObj = {};

  beforeEach(async () => {
    testObj.channelManager = await ChannelManager.new({ from: admin });
  });

  describe('createChannel', () => {
    const expectedEvent = 'ChannelCreated';

    it('should allow creating a new channel.', () => {
      const {
        channelManager: { createChannel, getChannelMetadata },
      } = testObj;

      return expectOneEventFromCall(
        expectedEvent,
        { channelId: 0, publisher: publisherA },
        createChannel(publisherXpub, publisherAIndex, channelMetadata, {
          from: publisherA,
        })
      ).then(() => expect(getChannelMetadata(0)).to.become(channelMetadata));
    });

    it('should create each channel with a different ID.', () => {
      const {
        channelManager: { createChannel },
      } = testObj;
      const expectedEvent = 'ChannelCreated';
      const channelMetadata = 'someMetadata';

      return createChannel(publisherXpub, publisherAIndex, channelMetadata, {
        from: publisherA,
      }).then(() =>
        expectOneEventFromCall(
          expectedEvent,
          { channelId: 1, publisher: publisherA },
          createChannel(publisherXpub, publisherAIndex, channelMetadata, {
            from: publisherA,
          })
        )
      );
    });
  });

  describe('Functios after channel is created', () => {
    beforeEach(async () => {
      const {
        logs: [
          {
            args: { channelId },
          },
        ],
      } = await testObj.channelManager.createChannel(
        publisherXpub,
        publisherAIndex,
        channelMetadata,
        {
          from: publisherA,
        }
      );

      testObj.channelId = channelId.toNumber();
    });

    describe('join', () => {
      const expectedEvent = 'PublisherJoined';

      it('should allow other publishers to join.', () => {
        const {
          channelManager: { join, isPublisher },
          channelId,
        } = testObj;

        return expect(isPublisher(channelId, publisherB))
          .to.become(false)
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              { channelId, publisher: publisherB },
              join(channelId, publisherBIndex, { from: publisherB })
            )
          )
          .then(() =>
            Promise.all([
              expect(isPublisher(channelId, publisherA)).to.become(true),
              expect(isPublisher(channelId, publisherB)).to.become(true),
            ])
          );
      });
    });

    describe('publish', () => {
      const someMessage = asciiToHex('IAmAMessageLol');
      const expectedEvent = 'MessagePublished';

      it('should allow publisher to publish a new message.', () => {
        const {
          channelManager: { publish },
          channelId,
        } = testObj;

        return expectOneEventFromCall(
          expectedEvent,
          { channelId, publisher: publisherA, messageId: 0 },
          publish(channelId, someMessage, { from: publisherA })
        );
      });

      it('should multiple publishers.', () => {
        const {
          channelManager: { join, publish },
          channelId,
        } = testObj;

        return join(channelId, publisherBIndex, { from: publisherB })
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              { channelId, publisher: publisherA, messageId: 0 },
              publish(channelId, someMessage, { from: publisherA })
            )
          )
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              { channelId, publisher: publisherB, messageId: 1 },
              publish(channelId, someMessage, { from: publisherB })
            )
          );
      });

      it('should publish each new message with a different ID.', () => {
        const {
          channelManager: { publish },
          channelId,
        } = testObj;

        return publish(channelId, someMessage, { from: publisherA }).then(() =>
          expectOneEventFromCall(
            expectedEvent,
            { channelId, publisher: publisherA, messageId: 1 },
            publish(channelId, someMessage, { from: publisherA })
          )
        );
      });

      it('should not allow publishing to an non-existent channel.', () => {
        const {
          channelManager: { publish },
          channelId,
        } = testObj;

        return expect(publish(channelId + 1, someMessage, { from: publisherA })).to.be.rejectedWith(
          Error,
          'Channel does not exist.'
        );
      });

      it('should not allow non-publisher to publish any message.', () => {
        const {
          channelManager: { publish },
          channelId,
        } = testObj;

        return expect(publish(channelId, someMessage, { from: admin })).to.be.rejectedWith(
          Error,
          'Only publishers are allowed to make this transaction.'
        );
      });
    });

    describe('subscribe and isSubscriber', () => {
      const messageTimeout = 60000;
      const numOfRetry = 3;
      const expectedEvent = 'ConsumerJoined';
      const xpub = asciiToHex(
        'xpub68Gmy5EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw'
      );
      const index = 10;

      it('should allow anyone to subscribe the channel.', () => {
        const {
          channelManager: { subscribe, isSubscriber },
          channelId,
        } = testObj;

        return expectOneEventFromCall(
          expectedEvent,
          { channelId, subscriber: subscriberA, consumerGroupId },
          subscribe(channelId, xpub, index, messageTimeout, numOfRetry, {
            from: subscriberA,
          })
        ).then(() =>
          Promise.all([
            expect(isSubscriber(channelId, subscriberA)).to.become(true),
            expect(isSubscriber(channelId, subscriberB)).to.become(false),
          ])
        );
      });
    });

    describe('getNext', () => {
      const expectedEvent = 'MessagePopped';
      const messageTimeout = 2;
      const numOfRetry = 2;
      const xpub = asciiToHex(
        'xpub68Gmy5EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw'
      );
      const anotherXpub = asciiToHex(
        'xpub79Gmy9EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw'
      );
      const index = 10;
      const skipDepth = 5;
      const firstMessage = asciiToHex('IMadeItFirst');
      const secondMessage = asciiToHex('IMadeItSecond');
      const thirdMessage = asciiToHex('IMadeItThird');
      const forthMessage = asciiToHex('IMadeItForth');

      beforeEach(() => {
        const {
          channelManager: { publish, subscribe },
          channelId,
        } = testObj;

        return Promise.all([
          publish(channelId, firstMessage, { from: publisherA }).then(() =>
            publish(channelId, secondMessage, { from: publisherA })
          ),
          subscribe(channelId, xpub, index, messageTimeout, numOfRetry, {
            from: subscriberA,
          }),
          subscribe(channelId, xpub, index, messageTimeout, numOfRetry, {
            from: subscriberB,
          }),
        ]);
      });

      it('should give out repeated message if timed out.', () => {
        const {
          channelManager: { getNext },
          channelId,
        } = testObj;

        return expectOneEventFromCall(
          expectedEvent,
          {
            channelId,
            subscriber: subscriberA,
            messageId: 0,
            consumerGroupId,
          },
          getNext(channelId, skipDepth, { from: subscriberA })
        )
          .then(() => sleep(messageTimeout))
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              {
                channelId,
                subscriber: subscriberB,
                messageId: 0,
                consumerGroupId,
              },
              getNext(channelId, skipDepth, { from: subscriberB })
            )
          );
      });

      it('should give out repeated message when too many read attempt is made.', () => {
        const {
          channelManager: { publish, getNext },
          channelId,
        } = testObj;

        return Promise.all([
          publish(channelId, thirdMessage, { from: publisherA }),
          publish(channelId, forthMessage, { from: publisherA }),
        ])
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              {
                channelId,
                subscriber: subscriberA,
                messageId: 0,
                consumerGroupId,
              },
              getNext(channelId, skipDepth, { from: subscriberA })
            )
          )
          .then(() => {
            expectOneEventFromCall(
              expectedEvent,
              {
                channelId,
                subscriber: subscriberA,
                messageId: 0,
                consumerGroupId,
              },
              getNext(channelId, skipDepth, { from: subscriberA })
            );
          });
      });

      it('should skip a message if it is requested too many times without confirm.', () => {
        const {
          channelManager: { publish, getNext },
          channelId,
        } = testObj;

        return Promise.all([
          getNext(channelId, skipDepth, { from: subscriberA }),
          getNext(channelId, skipDepth, { from: subscriberA }),
        ]).then(async () => {
          const { logs } = await getNext(channelId, skipDepth, {
            from: subscriberA,
          });

          expect(logs).to.have.lengthOf(2);
          const [eventMessageSkipped, eventMessagePopped] = logs;

          expect(eventMessageSkipped.event).to.equal('MessageSkipped');
          expect(propertiesBigNumberToNumber(eventMessageSkipped.args)).to.shallowDeepEqual({
            channelId,
            subscriber: subscriberA,
            messageId: 0,
            consumerGroupId,
          });

          expect(eventMessagePopped.event).to.equal('MessagePopped');
          expect(propertiesBigNumberToNumber(eventMessagePopped.args)).to.shallowDeepEqual({
            channelId,
            subscriber: subscriberA,
            messageId: 1,
            consumerGroupId,
          });
        });
      });

      it('should not allow being called when message queue is empty.', () => {
        const {
          channelManager: { getNext, confirm },
          channelId,
        } = testObj;

        return getNext(channelId, skipDepth, { from: subscriberA })
          .then(() => confirm(channelId, 0, { from: subscriberA }))
          .then(() => getNext(channelId, skipDepth, { from: subscriberA }))
          .then(() => confirm(channelId, 1, { from: subscriberA }))
          .then(() =>
            expect(getNext(channelId, skipDepth, { from: subscriberA })).to.be.rejectedWith(
              Error,
              'All messages are read for the moment.'
            )
          );
      });

      it('should not allow being called before subscribing.', () => {
        const {
          channelManager: { getNext },
          channelId,
        } = testObj;

        return expect(getNext(channelId, skipDepth, { from: subscriberC })).to.be.rejectedWith(
          Error,
          'Only subscriber is allowed to make this transaction.'
        );
      });
    });

    describe('confirm', () => {
      const expectedEvent = 'MessageRead';
      const messageTimeout = 2;
      const numOfRetry = 3;
      const xpub = asciiToHex(
        'xpub68Gmy5EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw'
      );
      const index = 10;
      const skipDepth = 5;
      const firstMessage = asciiToHex('IMadeItFirst');
      const secondMessage = asciiToHex('IMadeItSecond');
      const thirdMessage = asciiToHex('IMadeItThird');

      beforeEach(() => {
        const {
          channelManager: { publish, subscribe },
          channelId,
        } = testObj;

        return publish(channelId, firstMessage, { from: publisherA })
          .then(() => publish(channelId, secondMessage, { from: publisherA }))
          .then(() => publish(channelId, thirdMessage, { from: publisherA }))
          .then(() =>
            Promise.all([
              subscribe(channelId, xpub, index, messageTimeout, numOfRetry, {
                from: subscriberA,
              }),
              subscribe(channelId, xpub, index, messageTimeout, numOfRetry, {
                from: subscriberB,
              }),
            ])
          );
      });

      it('should be able to confirm that a message processed.', () => {
        const {
          channelManager: { getNext, confirm },
          channelId,
        } = testObj;

        return getNext(channelId, skipDepth, { from: subscriberA })
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              {
                channelId,
                subscriber: subscriberA,
                messageId: 0,
                consumerGroupId,
              },
              confirm(channelId, 0, { from: subscriberA })
            )
          )
          .then(() => sleep(messageTimeout))
          .then(() =>
            expectOneEventFromCall(
              'MessagePopped',
              {
                channelId,
                subscriber: subscriberA,
                messageId: 1,
                consumerGroupId,
              },
              getNext(channelId, skipDepth, { from: subscriberA })
            )
          );
      });

      it('should allow confirm a message after timeout by someone else.', () => {
        const {
          channelManager: { getNext, confirm },
          channelId,
        } = testObj;

        return expectOneEventFromCall(
          'MessagePopped',
          {
            channelId,
            subscriber: subscriberA,
            messageId: 0,
            consumerGroupId,
          },
          getNext(channelId, skipDepth, { from: subscriberA })
        )
          .then(() => sleep(messageTimeout))
          .then(() =>
            expectOneEventFromCall(
              'MessagePopped',
              {
                channelId,
                subscriber: subscriberB,
                messageId: 0,
                consumerGroupId,
              },
              getNext(channelId, skipDepth, { from: subscriberB })
            )
          )
          .then(() =>
            expect(confirm(channelId, 0, { from: subscriberA })).to.be.rejectedWith(
              Error,
              'Subscriber not allowed to confirm this message.'
            )
          )
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              {
                channelId,
                subscriber: subscriberB,
                messageId: 0,
                consumerGroupId,
              },
              confirm(channelId, 0, { from: subscriberB })
            )
          );
      });

      it('should not allow a different address to confirm a message another one marked as read.', () => {
        const {
          channelManager: { getNext, confirm },
          channelId,
        } = testObj;

        return getNext(channelId, skipDepth, { from: subscriberA }).then(() => {
          expect(confirm(channelId, 0, { from: subscriberB })).to.be.rejectedWith(
            Error,
            'Subscriber not allowed to confirm this message.'
          );
        });
      });
    });

    describe('skip', () => {
      const expectedEvent = 'MessageSkipped';
      const messageTimeout = 2;
      const numOfRetry = 3;
      const xpub = asciiToHex(
        'xpub68Gmy5EdvgibQVfPdqkBBCHxA5htiqg55crXYuXoQRKfDBFA1WEjWgP6LHhwBZeNK1VTsfTFUHCdrfp1bgwQ9xv5ski8PX9rL2dZXvgGDnw'
      );
      const index = 10;
      const skipDepth = 3;
      const firstMessage = asciiToHex('IMadeItFirst');
      const secondMessage = asciiToHex('IMadeItSecond');
      const thirdMessage = asciiToHex('IMadeItThird');

      beforeEach(() => {
        const {
          channelManager: { publish, subscribe },
          channelId,
        } = testObj;

        return publish(channelId, firstMessage, { from: publisherA })
          .then(() => publish(channelId, secondMessage, { from: publisherA }))
          .then(() => publish(channelId, thirdMessage, { from: publisherA }))
          .then(() =>
            Promise.all([
              subscribe(channelId, xpub, index, messageTimeout, numOfRetry, {
                from: subscriberA,
              }),
              subscribe(channelId, xpub, index, messageTimeout, numOfRetry, {
                from: subscriberB,
              }),
            ])
          );
      });

      it('should be able to skip that a message.', () => {
        const {
          channelManager: { getNext, skip },
          channelId,
        } = testObj;

        return getNext(channelId, skipDepth, { from: subscriberA })
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              {
                channelId,
                subscriber: subscriberA,
                messageId: 0,
                consumerGroupId,
              },
              skip(channelId, 0, { from: subscriberA })
            )
          )
          .then(() => getNext(channelId, skipDepth, { from: subscriberB }))
          .then(() =>
            expectOneEventFromCall(
              expectedEvent,
              {
                channelId,
                subscriber: subscriberB,
                messageId: 1,
                consumerGroupId,
              },
              skip(channelId, 1, { from: subscriberB })
            )
          )
          .then(() => sleep(skipDepth))
          .then(() =>
            expectOneEventFromCall(
              'MessagePopped',
              {
                channelId,
                subscriber: subscriberB,
                messageId: 0,
                consumerGroupId,
              },
              getNext(channelId, skipDepth, { from: subscriberB })
            )
          )
          .then(() =>
            expectOneEventFromCall(
              'MessagePopped',
              {
                channelId,
                subscriber: subscriberA,
                messageId: 1,
                consumerGroupId,
              },
              getNext(channelId, skipDepth, { from: subscriberA })
            )
          );
      });
    });
  });
});
