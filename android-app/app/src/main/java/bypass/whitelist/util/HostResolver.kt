package bypass.whitelist.util

import android.util.Log
import bypass.whitelist.tunnel.TunnelVpnService
import java.io.ByteArrayOutputStream
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import kotlin.random.Random

object HostResolver {
    private const val TAG = "RELAY"
    private val fallbackDnsServers = listOf("1.1.1.1", "8.8.8.8")

    fun resolveIPv4(hostname: String): String {
        val trimmed = hostname.trim()
        if (trimmed.isEmpty()) return ""

        val systemAnswers = try {
            InetAddress.getAllByName(trimmed).filterIsInstance<Inet4Address>()
        } catch (e: Exception) {
            Log.d(TAG, "resolveIPv4(system): $trimmed -> FAILED: ${e.message}")
            emptyList()
        }

        val usableSystem = systemAnswers.firstOrNull(::isUsablePublicIPv4)
        if (usableSystem != null) {
            val ip = usableSystem.hostAddress ?: ""
            Log.d(TAG, "resolveIPv4(system): $trimmed -> $ip")
            return ip
        }

        if (systemAnswers.isNotEmpty()) {
            Log.w(TAG, "resolveIPv4(system): rejecting answers for $trimmed -> ${systemAnswers.joinToString { it.hostAddress ?: "?" }}")
        }

        for (dnsServer in fallbackDnsServers) {
            val resolved = queryARecord(trimmed, dnsServer)?.takeIf(::isUsablePublicIPv4)
            if (resolved != null) {
                val ip = resolved.hostAddress ?: ""
                Log.d(TAG, "resolveIPv4(fallback $dnsServer): $trimmed -> $ip")
                return ip
            }
        }

        val fallbackSystem = systemAnswers.firstOrNull()?.hostAddress ?: ""
        if (fallbackSystem.isNotEmpty()) {
            Log.w(TAG, "resolveIPv4(system-fallback): $trimmed -> $fallbackSystem")
        }
        return fallbackSystem
    }

    private fun isUsablePublicIPv4(address: Inet4Address): Boolean {
        if (address.isAnyLocalAddress || address.isLoopbackAddress || address.isLinkLocalAddress ||
            address.isSiteLocalAddress || address.isMulticastAddress) {
            return false
        }

        val bytes = address.address
        val first = bytes[0].toInt() and 0xFF
        val second = bytes[1].toInt() and 0xFF

        if (first == 0 || first >= 224) return false
        if (first == 198 && (second == 18 || second == 19)) return false

        return true
    }

    private fun queryARecord(hostname: String, dnsServer: String): Inet4Address? {
        val query = buildDnsQuery(hostname)
        if (query.isEmpty()) return null

        return try {
            DatagramSocket().use { socket ->
                socket.soTimeout = 2000
                TunnelVpnService.instance?.protect(socket)

                val serverAddr = InetAddress.getByName(dnsServer)
                val requestPacket = DatagramPacket(query, query.size, serverAddr, 53)
                socket.send(requestPacket)

                val responseBuffer = ByteArray(1500)
                val responsePacket = DatagramPacket(responseBuffer, responseBuffer.size)
                socket.receive(responsePacket)

                parseDnsResponse(responseBuffer, responsePacket.length)
            }
        } catch (e: Exception) {
            Log.d(TAG, "resolveIPv4(fallback $dnsServer): $hostname -> FAILED: ${e.message}")
            null
        }
    }

    private fun buildDnsQuery(hostname: String): ByteArray {
        val labels = hostname.trim('.').split('.').filter { it.isNotEmpty() }
        if (labels.isEmpty()) return ByteArray(0)

        val out = ByteArrayOutputStream()
        val id = Random.nextInt(0, 0x10000)

        writeU16(out, id)
        writeU16(out, 0x0100)
        writeU16(out, 1)
        writeU16(out, 0)
        writeU16(out, 0)
        writeU16(out, 0)

        for (label in labels) {
            val bytes = label.toByteArray(Charsets.US_ASCII)
            if (bytes.isEmpty() || bytes.size > 63) return ByteArray(0)
            out.write(bytes.size)
            out.write(bytes)
        }
        out.write(0)
        writeU16(out, 1)
        writeU16(out, 1)

        return out.toByteArray()
    }

    private fun parseDnsResponse(buffer: ByteArray, length: Int): Inet4Address? {
        if (length < 12) return null

        val qdCount = readU16(buffer, 4)
        val anCount = readU16(buffer, 6)
        var offset = 12

        repeat(qdCount) {
            offset = skipDnsName(buffer, offset)
            if (offset + 4 > length) return null
            offset += 4
        }

        repeat(anCount) {
            offset = skipDnsName(buffer, offset)
            if (offset + 10 > length) return null

            val type = readU16(buffer, offset)
            val klass = readU16(buffer, offset + 2)
            val rdLength = readU16(buffer, offset + 8)
            val rdataOffset = offset + 10
            if (rdataOffset + rdLength > length) return null

            if (type == 1 && klass == 1 && rdLength == 4) {
                val addr = InetAddress.getByAddress(buffer.copyOfRange(rdataOffset, rdataOffset + 4))
                if (addr is Inet4Address) return addr
            }

            offset = rdataOffset + rdLength
        }

        return null
    }

    private fun skipDnsName(buffer: ByteArray, start: Int): Int {
        var offset = start
        while (offset < buffer.size) {
            val len = buffer[offset].toInt() and 0xFF
            if (len == 0) return offset + 1
            if ((len and 0xC0) == 0xC0) return offset + 2
            offset += 1 + len
        }
        return offset
    }

    private fun readU16(buffer: ByteArray, offset: Int): Int {
        return ((buffer[offset].toInt() and 0xFF) shl 8) or (buffer[offset + 1].toInt() and 0xFF)
    }

    private fun writeU16(out: ByteArrayOutputStream, value: Int) {
        out.write((value shr 8) and 0xFF)
        out.write(value and 0xFF)
    }
}
