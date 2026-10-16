class DomainRequestError(Exception):
    def __init__(self,status,data=None,message_id=None):
        self.status,self.data,self.message_id=status,data,message_id
        detail=data if isinstance(data,dict) else {}
        super().__init__(detail.get('reason') or detail.get('error') or status)

class DomainExecutionFailed(Exception):
    def __init__(self,payload):
        super().__init__(str(payload));self.payload=payload

