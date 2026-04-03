# whatsapp-mcp

MCP server for WhatsApp messaging via [whatsmeow](https://github.com/tulir/whatsmeow).

## Features

- Send and receive messages (text, images, documents)
- List chats and contacts
- Search messages
- Download attachments
- Read receipts and dismiss tracking
- Group info and management

## Prerequisites

- Go 1.24+
- A WhatsApp account (phone number for QR pairing)

## Install

```bash
go install github.com/newknew/whatsapp-mcp@main
```

## First run

On first launch, the server displays a QR code in the
terminal. Scan it with WhatsApp on your phone to pair.

```bash
whatsapp-mcp
```

The session is stored in
`~/.local/share/newknew-mcp/whatsapp/` and persists across
restarts.

## Usage

Runs as an HTTP server on `localhost:2053`. Health check:

```bash
curl http://localhost:2053/health
```

## Data paths

- Session DB: `~/.local/share/newknew-mcp/whatsapp/`
- Debug log: `~/.local/log/newknew-mcp/whatsapp.log`
- Downloaded files: `/tmp/newknew_whatsapp_*`

## License

MIT
