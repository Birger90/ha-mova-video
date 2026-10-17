# MOVA Video

Live-Video der Roboterkamera in Home Assistant für MOVA-/Dreame-Saugroboter, deren Kamera über **Tencent XP2P** streamt (Gerätefähigkeit `stream_tx`, z. B. MOVA Z70 Ultra Roller Complete).

Ergänzung zu [Tasshack/dreame-vacuum](https://github.com/Tasshack/dreame-vacuum): Die Integration nutzt dessen bestehende Cloud-Anmeldung und speichert **keine eigenen Zugangsdaten**.

> Status: experimentell (0.1.0). Getestet bisher nur offline (Verteiler, Provisionierung mit Testdaten).

## Funktionsweise

1. Holt über die Dreame-/MOVA-Cloud die Tencent-Video-Provisionierung des Roboters (`/dreame-third-video/tx/...`) – nur im Arbeitsspeicher, nicht geloggt.
2. Lädt beim ersten Start eine verifizierte XP2P-Laufzeit nach `config/.storage/mova_video/xp2p-runtime` (Tencent-SDK von Maven Central, Android-Laufzeitbibliotheken von android.googlesource.com, alles per SHA-256 geprüft; auf x86_64 zusätzlich qemu).
3. Startet einen kleinen nativen Worker, der einen lokalen HTTP-FLV-Strom bereitstellt.
4. Ein lokaler Verteiler gibt den Strom an Home Assistant (WebRTC/HLS) und an Standbilder weiter. Ohne Zuschauer wird nach 20 s gestoppt.

Unterstützte Hosts: Linux aarch64 (z. B. Raspberry Pi 4/5) und x86_64.

## Einrichtung

1. HACS → Benutzerdefinierte Repositories → `https://github.com/Birger90/ha-mova-video` (Integration) → installieren → Home Assistant neu starten.
2. Einstellungen → Geräte & Dienste → Integration hinzufügen → **MOVA Video** → Roboter wählen.
3. Die Kamera **Live-Video** erscheint am Gerät des Roboters.

Prüfen ohne Video: Aktion `mova_video.diagnose` (optional `runtime: true`) zeigt, ob die Cloud die Provisionierung liefert – ohne Geheimnisse.

## Herkunft und Lizenz

MIT. Die XP2P-Laufzeit (Bootstrap, vorkompilierter Worker, Credential-Ableitung) und das Protokoll stammen aus [EvotecIT/homeassistant-dreamelawnmower](https://github.com/EvotecIT/homeassistant-dreamelawnmower) (MIT) und wurden für Saugroboter angepasst. Kein offizielles Produkt von MOVA, Dreame oder Tencent.
