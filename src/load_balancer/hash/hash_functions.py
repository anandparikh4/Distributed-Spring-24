# hash functions for request and server in 3 pairs


import hashlib


'''
    Hash functions: Batch 0
    Note about hash functions: 
    As provided in problem statement
'''

def requestHash1(i: int) -> int:
    hash_int = i*i + 2*i + 17
    return hash_int

def serverHash1(i: int, j: int) -> int:
    hash_int = i*i + j*j + 2*j + 25
    return hash_int


'''
    Hash functions: Batch 1
    Note about hash functions: 
    The coefficients are - 
        1) Large : To cover the whole slot-space nearly uniformly with larger distance b/w virtual copies
        2) Prime : So finally taking modulo by any slot size does not reduce randomness
'''


def requestHash2(i: int) -> int:
    hash_int = 1427*i*i + 2503*i + 2003
    return hash_int

def serverHash2(i: int, j: int) -> int:
    hash_int = 1249*i*i + 2287*j*j + 1663*j + 2287
    return hash_int



'''
    Hash functions: Batch 2
    Note about hash functions: 
    Popular cryptographic hash algorithm SHA-256 has been used
'''

def requestHash3(i: int) -> int:
    hash_int = i*i + 2*i + 17
    hash_bytes = hashlib.sha256(hash_int.to_bytes(4 , 'big')).digest()
    hash_int = int.from_bytes(hash_bytes , 'big')
    return hash_int

def serverHash3(i: int, j: int) -> int:
    hash_int = i*i + j*j + 2*j + 25
    hash_bytes = hashlib.sha256(hash_int.to_bytes(4 , 'big')).digest()
    hash_int = int.from_bytes(hash_bytes , 'big')
    return hash_int


# Lists to store the functions
requestHashList = [requestHash1, requestHash2, requestHash3]
serverHashList = [serverHash1, serverHash2, serverHash3]