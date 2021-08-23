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
    NoData: 110
  }
}

constants.PG_VERSION = 0x00030000

const { INT4OID, VARCHAROID } = constants.fieldTypes

const messageNames = {}
Object.keys(constants.messageTypes).forEach(k => {
  messageNames[constants.messageTypes[k]] = k
})
constants.messageNames = messageNames

constants.BinaryInt = {
  format: constants.formats.Binary,
  oid: INT4OID
}

constants.VarChar = {
  format: constants.formats.Text,
  oid: VARCHAROID
}

function readCString (u8, off) {
  const start = off
  const len = u8.length
  while (u8[off] !== 0 && off < len) off++
  return u8.buffer.readString(off - start, start)
}

module.exports = { readCString, constants }
