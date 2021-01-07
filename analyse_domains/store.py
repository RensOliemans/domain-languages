class Store(dict):
    def add(self, language):
        if language in self:
            self[language] += 1
        else:
            self[language] = 1
