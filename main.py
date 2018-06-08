import web3
from web3 import Web3
from web3.middleware import geth_poa_middleware
import threading
import time
import psycopg2


def runner(n):
    """Запуск потока"""
    print(n)
    w = web3.Web3(web3.HTTPProvider('https://sidechain-dev.sonm.com'))

    w.middleware_stack.inject(geth_poa_middleware, layer=0)
    block = w.eth.getBlock(n, True)
    conn = psycopg2.connect("dbname='eth' user='ethtodb' host='localhost' password='ethtodb'")
    cur = conn.cursor()

    cur.execute("""insert into blocks(
                                    number,
                                    hash,
                                    parentHash,
                                    nonce,
                                    sha3Uncles,
                                    logsBloom,
                                    transactionsRoot,
                                    stateRoot,
                                    receiptsRoot,
                                    miner,
                                    difficulty,
                                    totalDifficulty,
                                    size,
                                    proofOfAuthorityData,
                                    gasLimit,
                                    gasUsed,
                                    timestamp,
                                    mixhash) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (block.number,
                 Web3.toHex(block.hash),
                 Web3.toHex(block.parentHash),
                 Web3.toHex(block.nonce),
                 Web3.toHex(block.sha3Uncles),
                 Web3.toHex(block.logsBloom),
                 Web3.toHex(block.transactionsRoot),
                 Web3.toHex(block.stateRoot),
                 Web3.toHex(block.receiptsRoot),
                 block.miner,
                 block.difficulty,
                 block.totalDifficulty,
                 block.size,
                 Web3.toHex(block.proofOfAuthorityData),
                 block.gasLimit,
                 block.gasUsed,
                 block.timestamp,
                 Web3.toHex(block.mixHash)))
    cur.close()
    for tr in block.transactions:
        trcur = conn.cursor()
        trcur.execute("""insert into transactions(
                                            hash,
                                            nonce ,
                                            blockHash,
                                            blockNumber ,
                                            transactionIndex,
                                            "from",
                                            "to",
                                            "value",
                                            gas,
                                            gasPrice,
                                            input,
                                            v,
                                            r,
                                            s) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                      (Web3.toHex(tr.hash),
                       tr.nonce,
                       Web3.toHex(tr.blockHash),
                       tr.blockNumber,
                       tr.transactionIndex,
                       tr['from'],
                       tr['to'],
                       tr['value'],
                       tr.gas,
                       tr.gasPrice,
                       tr.input,
                       tr.v,
                       Web3.toHex(tr.r),
                       Web3.toInt(tr.s)))
        trcur.close()
    conn.commit()
    conn.close()


def create_threads(maxth, slptime, startblock, endblock):
    i = startblock
    while i <= endblock:
        if threading.activeCount() <= maxth:
            my_thread = threading.Thread(target=runner, args=(i,))
            my_thread.start()
            i += 1
        else:
            time.sleep(slptime)


if __name__ == "__main__":
    w = web3.Web3(web3.HTTPProvider('https://sidechain-dev.sonm.com'))

    w.middleware_stack.inject(geth_poa_middleware, layer=0)
    firstblock = 1
    lastblock = w.eth.blockNumber
    print(str(firstblock) + "    " + str(lastblock))
    # create_threads(100, 0.1, firstblock, lastblock)
    create_threads(100, 0.01, 3024000, lastblock)
