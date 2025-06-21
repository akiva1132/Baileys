import { Boom } from '@hapi/boom'
import NodeCache from '@cacheable/node-cache'
import readline from 'readline'
import makeWASocket, { AnyMessageContent, BinaryInfo, delay, DisconnectReason, downloadAndProcessHistorySyncNotification, encodeWAM, fetchLatestBaileysVersion, getAggregateVotesInPollMessage, getHistoryMsg, isJidNewsletter, makeCacheableSignalKeyStore, proto, useMultiFileAuthState, WAMessageContent, WAMessageKey } from '../src'
//import MAIN_LOGGER from '../src/Utils/logger'
import open from 'open'
import fs from 'fs'
import P from 'pino'

const logger = P({ timestamp: () => `,"time":"${new Date().toJSON()}"` }, P.destination('./wa-logs.txt'))
logger.level = 'trace'

const doReplies = process.argv.includes('--do-reply')
const usePairingCode = process.argv.includes('--use-pairing-code')

// external map to store retry counts of messages when decryption/encryption fails
// keep this out of the socket itself, so as to prevent a message decryption/encryption loop across socket restarts
const msgRetryCounterCache = new NodeCache()

const onDemandMap = new Map<string, string>()

// Read line interface
const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
const question = (text: string) => new Promise<string>((resolve) => rl.question(text, resolve))

// start a connection
const startSock = async () => {
	const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info')
	// fetch latest version of WA Web
	const { version, isLatest } = await fetchLatestBaileysVersion()
	console.log(`using WA v${version.join('.')}, isLatest: ${isLatest}`)

	const sock = makeWASocket({
		version,
		logger,
		printQRInTerminal: !usePairingCode,
		auth: {
			creds: state.creds,
			/** caching makes the store faster to send/recv messages */
			keys: makeCacheableSignalKeyStore(state.keys, logger),
		},
		msgRetryCounterCache,
		generateHighQualityLinkPreview: true,
		// ignore all broadcast messages -- to receive the same
		// comment the line below out
		// shouldIgnoreJid: jid => isJidBroadcast(jid),
		// implement to handle retries & poll updates
		getMessage,
	})

	// Pairing code for Web clients
	if (usePairingCode && !sock.authState.creds.registered) {
		// todo move to QR event
		const phoneNumber = await question('Please enter your phone number:\n')
		const code = await sock.requestPairingCode(phoneNumber)
		console.log(`Pairing code: ${code}`)
	}

	const sendMessageWTyping = async (msg: AnyMessageContent, jid: string) => {
		await sock.presenceSubscribe(jid)
		await delay(500)

		await sock.sendPresenceUpdate('composing', jid)
		await delay(2000)

		await sock.sendPresenceUpdate('paused', jid)

		await sock.sendMessage(jid, msg)
	}

	// the process function lets you process all events that just occurred
	// efficiently in a batch
	sock.ev.process(
		// events is a map for event name => event data
		async (events) => {
			// something about the connection changed
			// maybe it closed, or we received all offline message or connection opened
			if (events['connection.update']) {
				const update = events['connection.update']
				const { connection, lastDisconnect } = update
				if (connection === 'close') {
					// reconnect if not logged out
					if ((lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut) {
						startSock()
					} else {
						console.log('Connection closed. You are logged out.')
					}
				}

				// WARNING: THIS WILL SEND A WAM EXAMPLE AND THIS IS A ****CAPTURED MESSAGE.****
				// DO NOT ACTUALLY ENABLE THIS UNLESS YOU MODIFIED THE FILE.JSON!!!!!
				// THE ANALYTICS IN THE FILE ARE OLD. DO NOT USE THEM.
				// YOUR APP SHOULD HAVE GLOBALS AND ANALYTICS ACCURATE TO TIME, DATE AND THE SESSION
				// THIS FILE.JSON APPROACH IS JUST AN APPROACH I USED, BE FREE TO DO THIS IN ANOTHER WAY.
				// THE FIRST EVENT CONTAINS THE CONSTANT GLOBALS, EXCEPT THE seqenceNumber(in the event) and commitTime
				// THIS INCLUDES STUFF LIKE ocVersion WHICH IS CRUCIAL FOR THE PREVENTION OF THE WARNING
				const sendWAMExample = false;
				if (connection === 'open' && sendWAMExample) {
					/// sending WAM EXAMPLE
					const {
						header: {
							wamVersion,
							eventSequenceNumber,
						},
						events,
					} = JSON.parse(await fs.promises.readFile("./boot_analytics_test.json", "utf-8"))

					const binaryInfo = new BinaryInfo({
						protocolVersion: wamVersion,
						sequence: eventSequenceNumber,
						events: events
					})

					const buffer = encodeWAM(binaryInfo);

					const result = await sock.sendWAMBuffer(buffer)
					console.log(result)
				}

				console.log('connection update', update)
			}

			// credentials updated -- save them
			if (events['creds.update']) {
				await saveCreds()
			}


			// history received
			// if(events['messaging-history.set']) {
			// 	const { chats, contacts, messages, isLatest, progress, syncType } = events['messaging-history.set']
			// 	if (syncType === proto.HistorySync.HistorySyncType.ON_DEMAND) {
			// 		console.log('received on-demand history sync, messages=', messages)
			// 	}
			// 	console.log(`recv ${chats.length} chats, ${contacts.length} contacts, ${messages.length} msgs (is latest: ${isLatest}, progress: ${progress}%), type: ${syncType}`)
			// }

			// received a new message
			if (events['messages.upsert']) {
				const upsert = events['messages.upsert']
				console.log(upsert.messages[0]?.key.remoteJid);
				let text;
				if (upsert?.messages[0]?.message) {
					text = upsert?.messages[0]?.message?.conversation;
				} else if (upsert?.messages[0]?.message?.extendedTextMessage) {
					text = upsert?.messages[0]?.message.extendedTextMessage.text;
				} else if (upsert?.messages[0]?.message?.buttonsMessage) {
					text = upsert?.messages[0]?.message.buttonsMessage.contentText;
				} else {
					return;
				}

				// if (upsert.messages[0]?.message?.conversation === 'ים חולון 70' && upsert.messages[0].key.remoteJid === "120363417918444815@g.us") {
				if (upsert.messages.length > 0) {
					console.log('found the message');
					
					const m = upsert.messages[0]
					// const messageTimestamp = m.timestamp && m.timestamp !== 0 ? m.timestamp : Date.now();
					const messageTimestamp = Date.now();
					try {
						await sock.chatModify(
							{
								clear: true,
								lastMessages: [{
									key: m.key,
									messageTimestamp: messageTimestamp
								}]
							},
							upsert.messages[0].key.remoteJid || ""
						);
					} catch (e) {
						console.error(e);
					}

				}
				console.log('recv messages')
			}

			if (events['chats.delete']) {
				console.log('chats deleted ', events['chats.delete'])
			}
		}
	)

	return sock

	async function getMessage(key: WAMessageKey): Promise<WAMessageContent | undefined> {
		// Implement a way to retreive messages that were upserted from messages.upsert
		// up to you

		// only if store is present
		return proto.Message.fromObject({})
	}
}

startSock()
