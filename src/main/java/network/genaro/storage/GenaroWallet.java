package network.genaro.storage;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.web3j.crypto.CipherException;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Wallet;
import org.web3j.crypto.WalletFile;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import static network.genaro.storage.CryptoUtil.sha256EscdaSign;

public final class GenaroWallet {
    private ECKeyPair ecKeyPair;

    public GenaroWallet(String v3Json, String password) throws CipherException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        WalletFile walletFile;
        try {
            walletFile = objectMapper.readValue(v3Json, WalletFile.class);
            ecKeyPair = Wallet.decrypt(password, walletFile);
        } catch (CipherException e) {
            throw new CipherException("Incorrect wallet password");
        } catch (IOException e) {
            throw new IOException("Bad json string input");
        }
    }

    public String signMessage(String message) throws NoSuchAlgorithmException {
        return sha256EscdaSign(ecKeyPair.getPrivateKey(), message);
    }

    public String getPublicKeyHexString() {
        return "04" + ecKeyPair.getPublicKey().toString(16);
    }

    public byte[] getPrivateKey() {
        return ecKeyPair.getPrivateKey().toByteArray();
    }
}
