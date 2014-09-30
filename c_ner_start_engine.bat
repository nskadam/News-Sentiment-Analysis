@echo off
java -mx1000m -cp "D:\0. Nilesh Files\7.1. Personal\12.3. News\3. Script\stanford-ner-2014-08-27\stanford-ner.jar" edu.stanford.nlp.ie.NERServer -loadClassifier "D:\0. Nilesh Files\7.1. Personal\12.3. News\3. Script\stanford-ner-2014-08-27\classifiers/english.muc.7class.distsim.crf.ser.gz" -port 8080 -outputFormat inlineXML
pause