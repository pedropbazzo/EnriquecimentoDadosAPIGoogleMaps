
/*
Cria a database se a mesma ainda não existe
*/
USE sys;

DROP SCHEMA IF EXISTS geolocalizacao;
CREATE DATABASE IF NOT EXISTS geolocalizacao DEFAULT CHARACTER SET = latin1 DEFAULT COLLATE = latin1_general_cs;

/* Criando o usuário 'geolocalizacao' e dando permissão à todos os objetos do banco de dados geolocalizacao */
DROP USER IF EXISTS geolocalizacao;
CREATE USER 'geolocalizacao'@'%' IDENTIFIED BY 'Geolxgeolocalizacao!177';
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE, SHOW VIEW, CREATE TEMPORARY TABLES ON geolocalizacao.* TO 'geolocalizacao'@'%' IDENTIFIED BY 'Geolx!177';

/**********************************************************************/
/************************* Tabelas Cadastrais *************************/
/**********************************************************************/
USE geolocalizacao;

CREATE TABLE geolocalizacao	(
							 id INT AUTO_INCREMENT PRIMARY KEY NOT NULL
							,latitude DECIMAL (20,10) NOT NULL
							,longitude DECIMAL (20,10) NOT NULL
							,rua VARCHAR(100) NULL
							,numero VARCHAR(20) NULL
							,bairro VARCHAR(150) NULL
							,cidade VARCHAR(150) NULL
							,cep VARCHAR(12) NULL
							,estado VARCHAR(150) NULL
							,pais VARCHAR(100) NULL
							,enderecocompleto VARCHAR(500) NULL
							,datacadastro DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
							,dataatualizacao DATETIME ON UPDATE CURRENT_TIMESTAMP
							);