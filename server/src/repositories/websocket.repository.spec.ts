import { Socket } from 'socket.io';
import { Mocked } from 'vitest';
import { EventRepository } from 'src/repositories/event.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { WebsocketRepository } from 'src/repositories/websocket.repository.js';
import { newConfigRepositoryMock } from 'test/repositories/config.repository.mock.js';
import { automock } from 'test/utils.js';

const newClient = () => {
  const client = {
    id: 'socket-id',
    join: vitest.fn().mockResolvedValue(undefined),
    leave: vitest.fn().mockResolvedValue(undefined),
    emit: vitest.fn(),
    disconnect: vitest.fn(),
    nsp: { name: '/' },
  };
  return { client, socket: client as unknown as Socket };
};

describe(WebsocketRepository.name, () => {
  let sut: WebsocketRepository;
  let eventMock: Mocked<EventRepository>;
  let loggerMock: Mocked<LoggingRepository>;

  beforeEach(() => {
    const loggerArg = { setContext: () => {} };
    // eslint-disable-next-line no-sparse-arrays
    eventMock = automock(EventRepository, { args: [, , loggerArg], strict: false });
    // eslint-disable-next-line no-sparse-arrays
    loggerMock = automock(LoggingRepository, { args: [, newConfigRepositoryMock()], strict: false });
    eventMock.emit.mockResolvedValue();
    sut = new WebsocketRepository(eventMock, loggerMock);
  });

  describe('handleConnection', () => {
    it('should join the user and session rooms and emit WebsocketConnect', async () => {
      const { client, socket } = newClient();
      sut.setAuthFn(() => Promise.resolve({ user: { id: 'user-id' }, session: { id: 'session-id' } } as any));

      await sut.handleConnection(socket);

      expect(client.join).toHaveBeenNthCalledWith(1, 'user-id');
      expect(client.join).toHaveBeenNthCalledWith(2, 'session-id');
      expect(eventMock.emit).toHaveBeenCalledWith('WebsocketConnect', { userId: 'user-id' });
      expect(client.disconnect).not.toHaveBeenCalled();
    });

    it('should not join a session room without a session', async () => {
      const { client, socket } = newClient();
      sut.setAuthFn(() => Promise.resolve({ user: { id: 'user-id' } } as any));

      await sut.handleConnection(socket);

      expect(client.join).toHaveBeenCalledTimes(1);
      expect(eventMock.emit).toHaveBeenCalledWith('WebsocketConnect', { userId: 'user-id' });
    });

    it('should reject the client and log the original error when authentication fails', async () => {
      const { client, socket } = newClient();
      const error = new Error('bad token');
      sut.setAuthFn(() => Promise.reject(error));

      await expect(sut.handleConnection(socket)).resolves.toBeUndefined();

      expect(loggerMock.error).toHaveBeenCalledWith(`Websocket connection error: ${error}`, error.stack);
      expect(client.emit).toHaveBeenCalledWith('error', 'unauthorized');
      expect(client.disconnect).toHaveBeenCalled();
      expect(eventMock.emit).not.toHaveBeenCalled();
    });

    it('should reject the client when emitting the connect event fails', async () => {
      const { client, socket } = newClient();
      sut.setAuthFn(() => Promise.resolve({ user: { id: 'user-id' } } as any));
      eventMock.emit.mockRejectedValue(new Error('boom'));

      await sut.handleConnection(socket);

      expect(client.emit).toHaveBeenCalledWith('error', 'unauthorized');
      expect(client.disconnect).toHaveBeenCalled();
    });
  });

  describe('handleDisconnect', () => {
    it('should leave the namespace room', async () => {
      const { client, socket } = newClient();

      await sut.handleDisconnect(socket);

      expect(client.leave).toHaveBeenCalledWith('/');
    });
  });
});
