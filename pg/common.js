const constants = {
  AuthenticationMD5Password: 5,
  formats: {
    Text: 0,
    Binary: 1
  },
  fieldTypes: {
    INT4OID: 23,
    VARCHAROID: 1043
  },
  messageTypes: {
    AuthenticationOk: 82,
    ErrorResponse: 69,
    RowDescription: 84,
    CommandComplete: 67,
    ParseComplete: 49,
    CloseComplete: 51,
    BindComplete: 50,
    ReadyForQuery: 90,
    BackendKeyData: 75,
    ParameterStatus: 83,
    ParameterDescription: 116,
    DataRow: 68,
    NoData: 110,
    NoticeResponse: 78
  },
  messageFields: {
    83: 'severity',
    67: 'sqlstate',
    77: 'message',
    68: 'detail',
    72: 'hint',
    80: 'position',
    113: 'internalquery',
    87: 'where',
    115: 'schema',
    116: 'table',
    99: 'column',
    100: 'datatype',
    110: 'constraint',
    70: 'filename',
    76: 'line',
    82: 'routine'
  },
  PG_VERSION: 0x00030000
}

const messageNames = {}
Object.keys(constants.messageTypes).forEach(k => {
  messageNames[constants.messageTypes[k]] = k
})
constants.messageNames = messageNames

constants.BinaryInt = {
  format: constants.formats.Binary,
  oid: constants.fieldTypes.INT4OID
}

constants.VarChar = {
  format: constants.formats.Text,
  oid: constants.fieldTypes.VARCHAROID
}

function readCString (u8, off) {
  const start = off
  const len = u8.length
  while (u8[off] !== 0 && off < len) off++
  return u8.buffer.readString(off - start, start)
}

module.exports = { readCString, constants }
