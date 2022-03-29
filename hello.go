package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// Le message json envoyé par la balance
type Msg struct {
	Id          string  `json:"id"`          // l'ID de la balance
	Timestamp   int32   `json:"timestamp"`   // le timestamp unix en secondes
	Weight      float32 `json:"weight"`      // le poids de la balance en Kg
	Temperature int32   `json:"temperature"` // la temperature à l'intérieur du boitier en °C
	Humidity    int32   `json:"humidity"`    // le taux d'humidité relative à l'intérieur du boitier en %
	Battery     float32 `json:"battery"`     // le niveau de la batterie en Volts
}

// callback handler = ici tu recevra le message posté par la balance
var msgHandler MQTT.MessageHandler = func(client MQTT.Client, raw MQTT.Message) {
	fmt.Printf("TOPIC: %s\n", raw.Topic())
	fmt.Printf("MSG: %s\n", raw.Payload())
	// Convertit le message binaire en json
	var msg Msg
	if err := json.Unmarshal(raw.Payload(), &msg); err != nil {
		fmt.Println(err)
	}
	fmt.Println("Received json MSG: ", msg)
}

func main() {
	broker := "e3bf5354fb5b4387842a9f321488d533.s2.eu.hivemq.cloud" // le serveur mqtt sur le cloud azure
	port := "8883"                                                  // le port tls
	topic := "balance"                                              // le nom du topic
	user := "XXX"
	password := "XXX"

	// Les paramètres de connections
	opts := MQTT.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("ssl://%s:%s", broker, port))
	opts.SetClientID("macbook")
	opts.SetUsername(user)
	opts.SetPassword(password)
	opts.SetDefaultPublishHandler(msgHandler)
	tlsConfig := NewTlsConfig()
	opts.SetTLSConfig(tlsConfig)

	// Crée le client MQTT
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("1. ", token.Error())
	}

	// Souscrit au topic balance et passe la callback msgHandler
	if token := client.Subscribe(topic, 0, msgHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	// Crée un message de test
	bytes := []byte(`{"id":"B1","timestamp":1648557797,"weight":68.02,"temperature":22,"humidity":45,"battery":4.2}`)

	// Publie le message de test sur le topic balance
	token := client.Publish(topic, 0, false, bytes)
	token.Wait()

	time.Sleep(10 * time.Second)

	client.Disconnect(250)
}

// Charge le certificat TLS pour se connecter au mqtt cloud HiveMQ
func NewTlsConfig() *tls.Config {
	certpool := x509.NewCertPool()
	ca, err := ioutil.ReadFile("/Users/goo/workspaces/hellogo/ca.crt")
	if err != nil {
		log.Fatalln(err.Error())
	}
	certpool.AppendCertsFromPEM(ca)
	return &tls.Config{
		RootCAs: certpool,
	}
}
