MSG_SIZE  = 64       
DELIMITER = ":"      
PADDING   = "x"      

TYPE_HELLO   = "HLO"  
TYPE_REQUEST = "REQ"  
TYPE_GRANT   = "GRT"  
TYPE_RELEASE = "REL"  


def encode_msg(msg_type: str, pid: int) -> bytes:
    """
    Codifica (tipo, pid) numa mensagem de tamanho fixo MSG_SIZE bytes.
    Exemplo (MSG_SIZE=64): 'REQ:3:xxxx...xxx'
    """
    payload = f"{msg_type}{DELIMITER}{pid}{DELIMITER}"
    return payload.ljust(MSG_SIZE, PADDING).encode()


def decode_msg(raw: bytes):
    """
    Decodifica bytes recebidos, retorna (tipo: str, pid: int).
    Remove o preenchimento 'x' antes de separar os campos.
    """
    text   = raw.decode().rstrip(PADDING)
    fields = text.split(DELIMITER)
    return fields[0], int(fields[1])
