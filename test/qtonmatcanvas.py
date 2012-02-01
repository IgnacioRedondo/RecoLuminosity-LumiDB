#!/usr/bin/env python
import sys, os, random
from numpy import arange, sin, pi
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from PyQt4 import QtGui, QtCore
from matplotlib.figure import Figure

progname = os.path.basename(sys.argv[0])

class MyMplCanvas(FigureCanvas):
    """this is a QWidget (as well as a FigureCanvasAgg, etc.)."""
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        # We want the axes cleared every time plot() is called
        self.axes.hold(False)
        self.compute_initial_figure()
        #
        FigureCanvas.__init__(self, fig)
        self.setParent(parent)
        FigureCanvas.setSizePolicy(self,
                                   QtGui.QSizePolicy.Expanding,
                                   QtGui.QSizePolicy.Expanding)
        FigureCanvas.updateGeometry(self)

    def compute_initial_figure(self):
        pass

class LumiCanvas(MyMplCanvas):
    def __init__(self,*args,**kwargs):
        MyMplCanvas.__init__(self,*args,**kwargs)
    def compute_initial_figure(self):
        t = arange(0.0,3.0,0.01)
        s = sin(2*pi*t)
        self.axes.plot(t, s)

class ApplicationWindow(QtGui.QMainWindow):
    '''main evt loop
    '''
    def __init__(self,width=5,height=4,dpi=100):
        QtGui.QMainWindow.__init__(self)
        self.setAttribute(QtCore.Qt.WA_DeleteOnClose)
        self.main_widget = QtGui.QWidget(self)
        l = QtGui.QVBoxLayout(self.main_widget)
        sc = LumiCanvas(self.main_widget,width=width,height=height,dpi=dpi)
        l.addWidget(sc)
        self.main_widget.setFocus()
        self.setCentralWidget(self.main_widget)

    def fileQuit(self):
        self.close()

    def closeEvent(self, ce):
        self.fileQuit()

if __name__ == "__main__":
    qApp = QtGui.QApplication(sys.argv)#every PyQt4 application must create an application object
    aw=ApplicationWindow(width=5,height=4,dpi=100)
    aw.show()
    sys.exit(qApp.exec_())

