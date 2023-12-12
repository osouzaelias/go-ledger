# go-ledger

## Definição do Objetivo

Avaliar custo beneficio entre inserir transações de débito e crédito, ou dada uma transação de crédito, 
ao incluir uma de débito o seu valor seja atualizado.

## Escopo e Limites

Esta fora do escopo desta poc tratar a idepotencia, pois em ambos os casos abaixo ela seria tratada em um processo
a parte.

### Caso 1

É executado 2 go routine, sendo que

- Para cada transação de *crédito* é gravado 4 registros
- Para cada transação de *débito* é gravado 1 registro 

### Caso 2

Não tem execução de go routine, sendo que:

- Para cada transação de *crédito* é gravado 4 registros, na sequencia o registro é atualizado com preço negativo e a 
operação atualizada para débito.


