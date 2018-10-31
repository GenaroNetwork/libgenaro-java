package network.genaro.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bouncycastle.util.encoders.Hex;
import org.web3j.crypto.CipherException;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Wallet;
import org.web3j.crypto.WalletFile;

import java.io.IOException;

import static network.genaro.storage.CryptoUtil.sha256EscdaSign;

public class GenaroWallet {
    private ECKeyPair ecKeyPair;

    public GenaroWallet(String v3Json, String password) throws CipherException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        WalletFile walletFile = null;
        try {
            walletFile = objectMapper.readValue(v3Json, WalletFile.class);
            ecKeyPair = Wallet.decrypt(password, walletFile);
        } catch (CipherException e) {
            throw new CipherException("incorrect wallet password");
        } catch (IOException e) {
            throw new IOException("bad json string input");
        }
    }

    public String signMessage(String message) {
        return sha256EscdaSign(ecKeyPair.getPrivateKey(), message);
    }

    public String getPublicKeyHexString() {
        return "04" + ecKeyPair.getPublicKey().toString(16);
    }

    public byte[] getPrivateKey() {
        return ecKeyPair.getPrivateKey().toByteArray();
    }
}
