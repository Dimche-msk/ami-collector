import yaml
import logging
import asyncio
from panoramisk import Manager, Message


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s-%(levelname)s-%(message)s',
    filename='app.log',
    filemode='a'  # Use 'w' to overwrite the file each time
)

def read_config(file_path="./config.yaml"):
    with open(file_path, 'r') as file:
        common_config = yaml.safe_load(file)
    return common_config

def _parse_string_to_dict(input_str):
        result = {}
        lines = input_str.split('\r\n')
        for line in lines:
            if line:
                key, value = line.split(':', 1)
                if key and value:
                    result[str(key.strip()).lower()] = value.strip()
        return result

class ASTERISK:
    def __init__(self, server_name, incoming_events_queue, outgoing_events_quqeue, config):
        self._config = config
        self.server_name = server_name
        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.secret = config['secret']
        self.incoming_events = incoming_events_queue
        self.outgoing_events = outgoing_events_quqeue if config['action'] else None
        self.manager = Manager(
                host=self.host,
                port=self.port,
                username=self.user,
                secret=self.secret,
                ping_delay=10,  # Delay after start
                ping_interval=10,  # Periodically ping AMI (dead or alive)
                reconnect_timeout=2,  # Timeout reconnect if connection lost
        )
        self.manager.on_connect = self.on_connect
        self.manager.on_login = self.on_login
        self.manager.on_disconnect = self.on_disconnect
        self.manager.register_event(pattern='*')(self.ami_callback)  # Register all events
        asyncio.create_task(self._connect())
        if config['action']:
            asyncio.create_task(self.send_actions())

    def __str__(self):
        return f'AMI:{self.server_name})'

    async def send_actions(self):
        def has_key_case_insensitive(d, _key):
            return any(k.lower() == _key.lower() for k in d.keys())

        while True:
            action_list = await self.outgoing_events.get()
            self.outgoing_events.task_done()
            output_dict = _parse_string_to_dict(action_list)
            if output_dict:
                if 'command' in output_dict and output_dict['command'].lower() == 'core restart now':
                    logging.warning(f'{self} Restarting Asterisk FORBIDDEN!')
                    message ={ "Response": "Error", "Message": "Restarting Asterisk FORBIDDEN!"}
                else:
                    message = await self.manager.send_action(output_dict)
                if not has_key_case_insensitive(output_dict, "ActionID"):
                    hide_id=True
                else:
                    hide_id=False
                logging.debug(f'{self} Message Response: {self._msg_to_str(message,hide_id)}')
                self.incoming_events.put_nowait(self._msg_to_str(message,hide_id))

    async def _connect(self):
        self.manager.connect(run_forever=True, on_startup=self.on_startup, on_shutdown=self.on_shutdown)

    def on_connect(self, mngr: Manager):
        logging.info(f'Connected to {self} successfully')

    def on_login(self, mngr: Manager):
        logging.info(f'Logged in to {self}')

    def on_disconnect(self, mngr: Manager, exc: Exception):
        logging.info(f'Disconnect from {self}')
        logging.debug(str(exc))

    async def on_startup(self, mngr: Manager):
        await asyncio.sleep(0.1)
        logging.info(f'Started connection to {self}')

    async def on_shutdown(self, mngr: Manager):
        await asyncio.sleep(0.1)
        logging.info(f'Shutdown AMI connection on {self}')

    def _msg_to_str(self, msg,hide_id=False):
        _msg_str=""
        if not isinstance(msg,list):
            msg = [msg,]
        for current_msg in msg:
            for param, value in current_msg.items():
                if isinstance(value,list):
                    for item in value:
                        _msg_str+=f'{param}: {item}\n'
                    continue
                elif param.lower() in ('content',) :
                    continue
                elif param.lower() == 'actionid' and hide_id:
                    continue
                _msg_str+=f'{param}: {value}\n'
            _msg_str+=f'\n'
        return _msg_str

    async def ami_callback(self, mngr: Manager, msg: Message):
        logging.debug(f'{self} Message RECEIVED: {self._msg_to_str(msg)}')
        if self.incoming_events.qsize() > 1000:
            logging.warning(f'{self} Message AMI queue exceeded 1000 messages. Clearing queue.')
            while not self.incoming_events.empty():
                self.incoming_events.get_nowait()
                self.incoming_events.task_done()
        self.incoming_events.put_nowait(self._msg_to_str(msg))


"""
server for listening port 5038, and emulating AMI server like asterisk AMI
"""
async def start_server():
    server = await asyncio.start_server(handle_client, read_config()['collector']['address'], read_config()['collector']['port'],)
    async with server:
        await server.serve_forever()

async def handle_client(reader, writer):


    async def _close_connection():
        # close connection
        writer.close()
        await writer.wait_closed()
        connection_semaphore.release()
        logging.info("CRM Connection closed by collector")

    async def login_procedure():
        message = await _read_message()
        logging.debug(f"Action from CRM: {message}")
        message_dict = _parse_string_to_dict(message)
        if all(key in message_dict for key in ("action", "username", "secret")):
            if str(message_dict["action"]).lower() == "login" and message_dict["username"] == read_config()["collector"]["user"] and message_dict["secret"] == str(read_config()["collector"]["secret"]):
                #if "Action: Login" in message and f'Username: {read_config()["collector"]["user"]}' in message and f'Secret: {read_config()["collector"]["secret"]}' in message:
                logging.info("Login procedure")
                await _send_message("Response: Success\r\nMessage: Authentication accepted\r\n\r\n")
                login_semaphore.release()
                return

        await _send_message("Response: Error\r\nMessage: Authentication failed\r\n\r\n")
        logging.debug(f'Login failed: {message}')
        await _close_connection()
        raise ValueError("Login failed")


    async def _read_message():
        buffer=""
        while "\r\n\r\n" not in buffer:
            data = await reader.read(1000)
            if not data:
                logging.error("Connection closed by the client")
                await _close_connection()
                raise ValueError("Connection closed by the client")
            try:
                buffer += data.decode()
            except UnicodeDecodeError:
                logging.error("Error decoding incoming message")
                await _close_connection()
                raise ValueError("Error decoding incoming message")
        message, delimiter = buffer.split("\r\n\r\n", 1)
        return message

    async def _send_message(msg):
        writer.write(str(msg).encode())
        await writer.drain()

    async def read_actions():
        while True:
            message = await _read_message()
            outgoing_events.put_nowait(message + "\r\n\r\n")


    async def write_ami_flow():
        while True:
            msg = await incoming_events.get()
            if msg is None:
                continue
            logging.debug(f"Sending message to CRM: {msg}")
            await _send_message(msg)
    #limit connection to 1
    if connection_semaphore.locked():
        await _send_message("Response: Error\r\nMessage: Connection limit reached")
        await _close_connection()
        await connection_semaphore.acquire()
        return

    await connection_semaphore.acquire()
    # wellcome message
    await _send_message("Asterisk Call Manager/7.2.0\r\n\r\n")
    logging.info("New CRM connection attempt ")
    try:
        await login_procedure()
    except ValueError:
        logging.error("Login failed")
        return
    logging.info("CRM connection: Login successful")
    try:
        await asyncio.gather(read_actions(), write_ami_flow(), return_exceptions=True)
    finally:
        await _close_connection()
        logging.info("CRM connection: closed")
    return


async def start_asterisk_connector():
    common_config = read_config()
    if not 'servers' in common_config:
        raise Exception('No servers in config file')
    asyncio.create_task(start_server())
    await login_semaphore.acquire()
    for server_name,aster_config in common_config['servers'].items():
        ASTERISK(server_name, incoming_events, outgoing_events, aster_config)
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    logging.info("Starting Asterisk AMI collector")
    incoming_events = asyncio.Queue()
    outgoing_events = asyncio.Queue()
    connection_semaphore = asyncio.Semaphore(1)
    login_semaphore = asyncio.Semaphore(0)
    asyncio.run(start_asterisk_connector())