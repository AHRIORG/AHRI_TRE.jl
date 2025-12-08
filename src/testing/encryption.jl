using Nettle
using Random
using SHA
using Base64

"""
    gen_key32_iv16(passphrase::Vector{UInt8}, salt::Vector{UInt8}) -> (key::Vector{UInt8}, iv::Vector{UInt8})

Derives a 32-byte AES key and a 16-byte IV from a passphrase and salt using SHA-256.
"""
function gen_key32_iv16(passphrase::Vector{UInt8}, salt::Vector{UInt8})
    # Concatenate passphrase + salt
    derived = sha256(passphrase ⋄ salt)
    key = derived[1:32]
    iv = derived[1:16]  # Use first 16 bytes as IV
    return key, iv
end
using UUIDs
using Dates

function encrypt_file_aes256cbc(file_path::String, passphrase::String)
    salt = rand(UInt8, 8)  # Random 8-byte salt
    key, iv = gen_key32_iv16(Vector{UInt8}(passphrase), salt)

    # Read file content
    plaintext = read(file_path)
    padded = add_padding_PKCS5(plaintext, 16)

    # Encrypt
    enc = Encryptor("AES256", key)
    ciphertext = encrypt(enc, :CBC, iv, padded)

    # Write ciphertext to file
    encrypted_path = file_path * ".enc"
    open(encrypted_path, "w") do io
        write(io, ciphertext)
    end

    # Metadata
    metadata = Dict(
        "file_id" => string(uuid4()),
        "file_path" => encrypted_path,
        "salt" => base64encode(salt),
        "encryption_algo" => "AES-256-CBC with PKCS5",
        "created_at" => string(now())
    )

    return metadata
end

function decrypt_file_aes256cbc(encrypted_path::String, passphrase::String, salt_b64::String)
    salt = base64decode(salt_b64)
    key, iv = gen_key32_iv16(Vector{UInt8}(passphrase), salt)

    ciphertext = read(encrypted_path)

    dec = Decryptor("AES256", key)
    padded_plaintext = decrypt(dec, :CBC, iv, ciphertext)
    plaintext = trim_padding_PKCS5(padded_plaintext)

    return plaintext
end