from blocks.db import BlockModel
from blocks.config import DSN, LOGGER

log = LOGGER.getChild('db')

BLOCK_CHUNK_SIZE = 50000


def compare_blocks(a, b):
    print('{} > {} = {}'.format(a.block_timestamp, b.block_timestamp, a.block_timestamp > b.block_timestamp))
    return a.block_timestamp > b.block_timestamp


def compare_block_window(a, b, c):
    # print('{}:{} > {}:{} > {}:{}'.format(
    #     a.block_number,
    #     a.block_timestamp,
    #     b.block_number,
    #     b.block_timestamp,
    #     c.block_number,
    #     c.block_timestamp,
    # ))
    return compare_blocks(a, b) and compare_blocks(b, c)


class BlockWindow:
    def __init__(self, size=3):
        self.values = []
        self.size = size

    def new(self, val):
        self.values.insert(0, val)

        if len(self.values) > self.size:
            self.values.pop()

    def validate(self):
        if not self.full():
            return False

        return compare_block_window(self.values[0], self.values[1], self.values[2])

        for i in range(1, len(self.values) - 1):
            if not compare_block_window(self.values[i-1], self.values[i], self.values[i+1]):
                return False

        return True

    def pick_invalid(self):
        if not self.full():
            return None

        if not compare_block_window(self.values[0], self.values[1], self.values[2]):
            print('failed compare_block_window')
            if (
                compare_blocks(self.values[0], self.values[2])
                and (
                    not compare_blocks(self.values[0], self.values[1])
                    or not compare_blocks(self.values[1], self.values[2])
                )
            ):
                print('a:', self.values[0].block_timestamp)
                print('b:', self.values[1].block_timestamp)
                print('c:', self.values[2].block_timestamp)
                return self.values[1]

        # for i in range(1, len(self.values) - 1):
        #     # Only middle values so we have at least two points of comparison
        #     if not compare_block_window(self.values[i-1], self.values[i], self.values[i+1]):
        #         print('failed compare_block_window')
        #         if (
        #             compare_blocks(self.values[i-1], self.values[i+1])
        #             and (
        #                 not compare_blocks(self.values[i-1], self.values[i])
        #                 or not compare_blocks(self.values[i], self.values[i+1])
        #             )
        #         ):
        #             print('a:', self.values[i-1].block_timestamp)
        #             print('b:', self.values[i].block_timestamp)
        #             print('c:', self.values[i+1].block_timestamp)
        #             return self.values[i]

        return None

    def full(self):
        return len(self.values) == self.size


def blocktime_anomalies(start=0, end=None):
    """ Look for anamalies in block times """
    model = BlockModel(DSN)

    if not end:
        end = model.get_latest()

    if start > end:
        raise Exception('Invalid start or end')

    chunks = int(end / BLOCK_CHUNK_SIZE) + 1
    window = BlockWindow()
    invalid_blocks = []
    invalid_block_counter = 0

    for i in range(0, chunks):
        chunk_start = i * BLOCK_CHUNK_SIZE
        chunk_end = chunk_start + BLOCK_CHUNK_SIZE

        #print('chunk: {}-{}'.format(chunk_start, chunk_end))

        for block in model.get_blocks(chunk_start, chunk_end):
            #print('block:', block.block_number)
            window.new(block)

            if window.full() and not window.validate():
                invalid_block = window.pick_invalid()
                if invalid_block:
                    invalid_blocks.append(invalid_block)
                    invalid_block_counter += 1
                    print('ANOMALY: {}'.format(
                        invalid_block
                    ))

    return {
        "invalid_block_counter": invalid_block_counter,
        "invalid_blocks": invalid_blocks,
    }


def add_subparser(subparsers):
    """ Add our CLI options to ArgumentParser submodules """
    bt_parser = subparsers.add_parser('blocktime', help='Verify block timestamps')
    bt_parser.add_argument('-s', '--start', default='0', help='Start of block range')
    bt_parser.add_argument('-e', '--end', default='latest', help='End of block range')


def run_cli(args):
    """ Run cli utility """
    if args.start:
        start = int(args.start)

    if args.end:
        try:
            end = int(args.end)
        except ValueError:
            end = None

    res = blocktime_anomalies(start=start, end=end)

    print('Found {} timestamp anomalies'.format(res.get('invalid_block_counter', 0)))
