# problemas conhecidos

1) Auto-complete de imports do VSCODE n funciona com o package local do InteligenteEtl

   Resolução: No arquivo settings.json do VScode adicione esse pedaço de código:

   ```json
      "python.autoComplete.extraPaths": [
         "${workspaceFolder}/InteligenteEtl"
      ]
   ```